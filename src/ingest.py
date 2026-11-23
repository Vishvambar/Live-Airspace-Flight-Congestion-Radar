import os
from datetime import datetime, timezone
from pathlib import Path
import requests
import pandas as pd

# 1. Configuration: OpenSky endpoint and Indian Airspace bounding box
OPENSKY_URL = "https://opensky-network.org/api/states/all"
INDIA_BBOX = {
    "lamin": 6.0,
    "lamax": 37.0,
    "lomin": 68.0,
    "lomax": 97.5,
    "extended": 1,  # Includes the 18th field: aircraft category
}

# Exact 18 positional columns returned by OpenSky /states/all?extended=1
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

BRONZE_DIR = Path("data/bronze")


def fetch_opensky_snapshot() -> dict:
    """Calls OpenSky REST API and returns raw JSON payload."""
    response = requests.get(OPENSKY_URL, params=INDIA_BBOX, timeout=15)
    response.raise_for_status()  # Fails loudly on 429 (rate limit) or 5xx errors
    return response.json()


def save_to_bronze(payload: dict) -> Path:
    """Converts raw state vectors to a DataFrame and writes an immutable Parquet file."""
    snapshot_time = payload.get("time")
    states = payload.get("states") or []

    if not states:
        raise ValueError("OpenSky returned 0 aircraft states. Check API status or bbox.")

    # Guard against OpenSky returning 17 columns if extended flag is ignored
    col_count = len(states[0])
    df = pd.DataFrame(states, columns=COLUMNS[:col_count])

    # Prevent Parquet null-type mismatch across snapshots
    if "sensors" in df.columns:
        df["sensors"] = df["sensors"].astype(str)

    # Stamp pipeline metadata
    df["snapshot_time"] = int(snapshot_time)
    df["ingested_at_utc"] = datetime.now(timezone.utc).isoformat()

    # Ensure Bronze directory exists and write Parquet
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = BRONZE_DIR / f"states_{snapshot_time}.parquet"
    df.to_parquet(output_path, index=False, engine="pyarrow")

    return output_path


def main():
    print("Fetching live airspace telemetry from OpenSky...")
    payload = fetch_opensky_snapshot()
    output_path = save_to_bronze(payload)
    row_count = len(payload.get("states") or [])
    print(f"Saved {row_count} raw aircraft records to {output_path}")


if __name__ == "__main__":
    main()