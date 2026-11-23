"""
Bronze Layer - Raw ADS-B Aircraft Telemetry Ingestion
Fetches live aircraft state vectors from OpenSky Network API,
saves immutable raw Parquet snapshots to data/bronze/,
and appends raw rows to bronze_states table in DuckDB.
"""

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple
import duckdb
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
DB_PATH = DATA_DIR / "airspace.duckdb"

OPENSKY_URL = "https://opensky-network.org/api/states/all"
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

# Indian Airspace Bounding Box
INDIA_BBOX = {
    "lamin": 6.0,
    "lamax": 37.0,
    "lomin": 68.0,
    "lomax": 97.5,
    "extended": 1,  # Requests 18th field: aircraft category
}

# 18 Positional columns returned by OpenSky /states/all?extended=1
COLUMNS = [
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
    "category",
]


def get_auth_headers_or_params() -> Tuple[dict, Optional[Tuple[str, str]]]:
    """Resolves authentication for OpenSky: OAuth2 Bearer token, Basic Auth, or Anonymous."""
    client_id = os.getenv("OPENSKY_CLIENT_ID")
    client_secret = os.getenv("OPENSKY_CLIENT_SECRET")
    username = os.getenv("OPENSKY_USERNAME")
    password = os.getenv("OPENSKY_PASSWORD")

    headers = {}
    auth = None

    if client_id and client_secret:
        try:
            token_resp = requests.post(
                TOKEN_URL,
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
                timeout=10,
            )
            if token_resp.status_code == 200:
                access_token = token_resp.json().get("access_token")
                headers["Authorization"] = f"Bearer {access_token}"
                return headers, None
        except Exception as e:
            print(f"[Bronze] OAuth2 token request failed: {e}. Falling back...")

    if username and password:
        auth = (username, password)

    return headers, auth


def fetch_opensky_snapshot() -> dict:
    """Calls OpenSky REST API with Indian airspace bounding box and timeout=15s."""
    headers, auth = get_auth_headers_or_params()
    response = requests.get(
        OPENSKY_URL,
        params=INDIA_BBOX,
        headers=headers,
        auth=auth,
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def save_to_bronze(payload: dict) -> Tuple[Path, int, int]:
    """
    Parses payload, writes immutable raw Parquet snapshot,
    and appends raw data to bronze_states table in DuckDB.
    Returns (parquet_path, snapshot_time, row_count).
    """
    snapshot_time = payload.get("time")
    states = payload.get("states") or []

    if not snapshot_time:
        snapshot_time = int(datetime.now(timezone.utc).timestamp())

    if not states:
        raise ValueError("OpenSky returned 0 aircraft states. Check bounding box or network.")

    col_count = len(states[0])
    df = pd.DataFrame(states, columns=COLUMNS[:col_count])

    # Convert object/null columns for schema consistency in Parquet & DuckDB
    if "sensors" in df.columns:
        df["sensors"] = df["sensors"].astype(str)
    if "callsign" in df.columns:
        df["callsign"] = df["callsign"].astype(str)
    if "squawk" in df.columns:
        df["squawk"] = df["squawk"].astype(str)

    # Stamp pipeline metadata
    df["snapshot_time"] = int(snapshot_time)
    df["ingested_at_utc"] = datetime.now(timezone.utc).isoformat()

    # Ensure bronze directory exists
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Save raw Parquet (immutable snapshot)
    parquet_path = BRONZE_DIR / f"states_{snapshot_time}.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")

    # 2. Append to DuckDB bronze_states table
    conn = duckdb.connect(str(DB_PATH))
    try:
        tables = [t[0] for t in conn.cursor().execute("SHOW TABLES").fetchall()]
        if "bronze_states" not in tables:
            conn.register("df_snapshot", df)
            conn.cursor().execute("CREATE TABLE bronze_states AS SELECT * FROM df_snapshot")
        else:
            existing_snap = conn.cursor().execute(
                "SELECT COUNT(*) FROM bronze_states WHERE snapshot_time = ?",
                [int(snapshot_time)],
            ).fetchall()[0][0]
            if existing_snap == 0:
                conn.register("df_snapshot", df)
                conn.cursor().execute("INSERT INTO bronze_states SELECT * FROM df_snapshot")
            else:
                print(f"[Bronze] Snapshot {snapshot_time} already present in bronze_states. Skipped DB duplicate append.")
    finally:
        conn.close()

    return parquet_path, int(snapshot_time), len(df)


def ingest_existing_parquet_files() -> int:
    """Helper to backfill any existing bronze Parquet files into bronze_states table in DuckDB."""
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    parquet_files = sorted(BRONZE_DIR.glob("states_*.parquet"))
    if not parquet_files:
        return 0

    conn = duckdb.connect(str(DB_PATH))
    try:
        tables = [t[0] for t in conn.cursor().execute("SHOW TABLES").fetchall()]
        if "bronze_states" not in tables:
            first = True
            for pf in parquet_files:
                if first:
                    conn.cursor().execute(f"CREATE TABLE bronze_states AS SELECT * FROM read_parquet('{pf.as_posix()}')")
                    first = False
                else:
                    conn.cursor().execute(f"INSERT INTO bronze_states SELECT * FROM read_parquet('{pf.as_posix()}')")
            print(f"[Bronze] Backfilled {len(parquet_files)} parquet files into bronze_states.")
        else:
            # Check for each file if snapshot already present
            loaded = 0
            for pf in parquet_files:
                stem = pf.stem
                try:
                    snap_time = int(stem.replace("states_", ""))
                except ValueError:
                    continue
                cnt = conn.cursor().execute("SELECT COUNT(*) FROM bronze_states WHERE snapshot_time = ?", [snap_time]).fetchall()[0][0]
                if cnt == 0:
                    conn.cursor().execute(f"INSERT INTO bronze_states SELECT * FROM read_parquet('{pf.as_posix()}')")
                    loaded += 1
            if loaded > 0:
                print(f"[Bronze] Backfilled {loaded} additional parquet snapshots into bronze_states.")
    finally:
        conn.close()
    return len(parquet_files)


def run_ingest() -> Tuple[Path, int, int]:
    """Runs a single live fetch from OpenSky and writes to Bronze."""
    payload = fetch_opensky_snapshot()
    return save_to_bronze(payload)


def main():
    print("Fetching live airspace telemetry from OpenSky Network...")
    try:
        parquet_path, snap_time, count = run_ingest()
        print(f"[Bronze Success] Saved {count} raw aircraft records to {parquet_path}")
        print(f"[Bronze Success] Snapshot Time: {snap_time}")
    except Exception as e:
        print(f"[Bronze Warning] Live fetch failed ({e}). Checking local bronze backups...")
        bf_count = ingest_existing_parquet_files()
        print(f"[Bronze] Total {bf_count} local parquet files available.")
        return

    # Verify DuckDB count
    conn = duckdb.connect(str(DB_PATH))
    total_bronze = conn.cursor().execute("SELECT COUNT(*) FROM bronze_states").fetchall()[0][0]
    conn.close()
    print(f"[Bronze DuckDB] Total records in bronze_states table: {total_bronze}")


if __name__ == "__main__":
    main()
