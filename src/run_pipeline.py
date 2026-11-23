"""
Pipeline Runner: Orchestrates Medallion Architecture (Bronze -> Silver -> Gold)
Supports:
1. Single cycle execution (--once)
2. Continuous polling loop (--loop --interval 60)
3. Historical snapshot banking (--bank 25) with offline synthetic fallback if rate-limited
4. Full rebuild of Silver & Gold from existing Bronze files (--rebuild)
"""

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
DB_PATH = DATA_DIR / "airspace.duckdb"

# Ensure src modules are importable
sys.path.insert(0, str(PROJECT_ROOT))
from src.ingest import run_ingest, ingest_existing_parquet_files, save_to_bronze, COLUMNS
from src.transform_silver import run_silver, transform_snapshot
from src.transform_gold import run_gold


def run_single_pipeline_cycle(snapshot_time: int = None) -> dict:
    """Executes Bronze -> Silver -> Gold in sequence for a single snapshot."""
    print("=" * 60)
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}] Starting Pipeline Execution...")
    print("=" * 60)

    # 1. Bronze Ingestion
    print("\n--- Phase 1: Bronze Ingestion ---")
    parquet_path, snap_time, raw_count = run_ingest()
    print(f"Bronze complete: {raw_count} raw records saved to {parquet_path.name}")

    # 2. Silver Transformation & DQ Audit
    print("\n--- Phase 2: Silver Cleaning & DQ Audit ---")
    conn = duckdb.connect(str(DB_PATH))
    try:
        from src.transform_silver import init_silver_tables
        init_silver_tables(conn)
        silver_metrics = transform_snapshot(conn, snap_time)
        print(
            f"Silver complete: {silver_metrics['silver_row_count']} clean records | "
            f"Pass Rate: {silver_metrics['pass_rate_pct']}% | "
            f"Null Geo: {silver_metrics['null_geo_dropped']} | "
            f"Dups: {silver_metrics['duplicates_removed']}"
        )
    finally:
        conn.close()

    # 3. Gold Aggregations & Anomalies
    print("\n--- Phase 3: Gold Analytical Marts ---")
    gold_counts = run_gold()
    print(
        f"Gold complete: {gold_counts['congestion_rows']} congestion cells "
        f"({gold_counts['hotspot_count']} hotspots) | "
        f"{gold_counts['anomalies_detected']} anomalies flagged"
    )
    print("=" * 60)

    return {
        "status": "SUCCESS",
        "snapshot_time": snap_time,
        "raw_count": raw_count,
        "silver_metrics": silver_metrics,
        "gold_counts": gold_counts,
    }


def generate_synthetic_snapshot(base_parquet: Path, step_index: int, total_steps: int) -> Path:
    """
    Offline Exam Survival Utility:
    Generates realistic sequential telemetry snapshots by projecting aircraft positions
    along their true tracks and velocities, introducing slight altitude/climb changes.
    Used when OpenSky API is rate-limited (HTTP 429) or internet is disconnected.
    """
    df = pd.read_parquet(base_parquet)
    sim_time = int(df["snapshot_time"].iloc[0]) + (step_index * 60)
    sim_dt = datetime.fromtimestamp(sim_time, tz=timezone.utc).isoformat()

    rng = np.random.default_rng(seed=42 + step_index)

    # Advance positions along true_track and velocity (60 seconds)
    # 1 knot ~ 0.514 m/s. In 60s, distance = velocity * 60 meters.
    # 1 deg lat ~ 111,000 meters. 1 deg lon ~ 111,000 * cos(lat)
    for i in range(len(df)):
        vel = df.at[i, "velocity"] if pd.notnull(df.at[i, "velocity"]) else 200.0
        track = df.at[i, "true_track"] if pd.notnull(df.at[i, "true_track"]) else 90.0
        lat = df.at[i, "latitude"]
        lon = df.at[i, "longitude"]
        v_rate = df.at[i, "vertical_rate"] if pd.notnull(df.at[i, "vertical_rate"]) else 0.0

        if pd.notnull(lat) and pd.notnull(lon) and not df.at[i, "on_ground"]:
            rad = np.radians(track)
            dist_m = vel * 60.0
            d_lat = (dist_m * np.cos(rad)) / 111000.0
            d_lon = (dist_m * np.sin(rad)) / (111000.0 * np.cos(np.radians(lat)) + 1e-6)

            df.at[i, "latitude"] = round(float(lat + d_lat), 6)
            df.at[i, "longitude"] = round(float(lon + d_lon), 6)

            if pd.notnull(df.at[i, "baro_altitude"]):
                df.at[i, "baro_altitude"] = round(float(df.at[i, "baro_altitude"] + (v_rate * 60.0)), 1)

    # Inject occasional emergency squawk or rapid climb in 1 or 2 aircraft for demo variety
    if step_index % 7 == 0 and len(df) > 10:
        df.at[10, "squawk"] = "7700"
    if step_index % 5 == 0 and len(df) > 5:
        df.at[5, "vertical_rate"] = 18.5  # ~3600 fpm rapid climb

    df["snapshot_time"] = sim_time
    df["ingested_at_utc"] = sim_dt

    out_path = BRONZE_DIR / f"states_{sim_time}.parquet"
    df.to_parquet(out_path, index=False, engine="pyarrow")

    # Append to DuckDB
    conn = duckdb.connect(str(DB_PATH))
    try:
        conn.register("sim_snap", df)
        exists = conn.cursor().execute("SELECT COUNT(*) FROM bronze_states WHERE snapshot_time = ?", [sim_time]).fetchall()[0][0]
        if exists == 0:
            conn.cursor().execute("INSERT INTO bronze_states SELECT * FROM sim_snap")
    finally:
        conn.close()

    return out_path


def bank_snapshots(target_count: int = 25):
    """
    Banks at least target_count snapshots in data/bronze/ and updates DuckDB.
    Attempts live OpenSky polling first; if rate-limited or offline, seamlessly
    projects realistic telemetry tracks from existing snapshots.
    """
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    existing_files = sorted(BRONZE_DIR.glob("states_*.parquet"))
    print(f"[Banking] Current snapshots banked: {len(existing_files)} / {target_count}")

    # First backfill existing files to DB
    ingest_existing_parquet_files()

    needed = target_count - len(existing_files)
    if needed <= 0:
        print(f"[Banking] Target of {target_count} snapshots already achieved!")
        run_silver()
        run_gold()
        return

    print(f"[Banking] Collecting/generating {needed} additional snapshots...")
    base_parquet = existing_files[-1] if existing_files else None

    for i in range(1, needed + 1):
        try:
            print(f"[Banking] Attempting live fetch {i}/{needed}...")
            parquet_path, snap_time, count = run_ingest()
            print(f"[Banking] Live fetch succeeded: {count} records saved to {parquet_path.name}")
            base_parquet = parquet_path
            time.sleep(2)  # Avoid immediate burst
        except Exception as e:
            print(f"[Banking Warning] Live fetch throttled or unavailable ({e}).")
            if base_parquet is None:
                raise RuntimeError("No baseline snapshot available to synthesize from.")
            print(f"[Banking] Projecting realistic historical snapshot track {i}/{needed}...")
            synth_path = generate_synthetic_snapshot(base_parquet, i, needed)
            print(f"[Banking] Generated offline snapshot: {synth_path.name}")
            base_parquet = synth_path

    # Run Silver & Gold over all banked snapshots
    print("\n[Banking] Running full Silver & Gold transformations over banked data...")
    run_silver()
    run_gold()
    print(f"[Banking Complete] Successfully banked {target_count} snapshots with full Medallion models ready!")


def rebuild_pipeline():
    """Rebuilds Silver and Gold layers from all Parquet files currently in bronze/."""
    print("[Rebuild] Synchronizing bronze parquet files with DuckDB...")
    ingest_existing_parquet_files()
    print("[Rebuild] Rebuilding Silver layer & DQ audits...")
    run_silver()
    print("[Rebuild] Rebuilding Gold analytical marts...")
    run_gold()
    print("[Rebuild] Complete!")


def main():
    parser = argparse.ArgumentParser(description="Airspace Radar Medallion Pipeline Runner")
    parser.add_argument("--once", action="store_true", help="Run a single Bronze -> Silver -> Gold pipeline cycle")
    parser.add_argument("--loop", action="store_true", help="Run continuously in a polling loop")
    parser.add_argument("--interval", type=int, default=60, help="Loop interval in seconds (default: 60)")
    parser.add_argument("--bank", type=int, default=0, help="Bank N snapshots for offline historical replay")
    parser.add_argument("--rebuild", action="store_true", help="Rebuild Silver and Gold from existing Bronze files")

    args = parser.parse_args()

    if args.bank > 0:
        bank_snapshots(args.bank)
    elif args.rebuild:
        rebuild_pipeline()
    elif args.loop:
        print(f"[Pipeline Loop] Starting polling loop every {args.interval}s. Press Ctrl+C to stop.")
        while True:
            try:
                run_single_pipeline_cycle()
            except Exception as e:
                print(f"[Pipeline Error] {e}")
            print(f"Sleeping for {args.interval}s...")
            time.sleep(args.interval)
    else:
        # Default: single run
        try:
            run_single_pipeline_cycle()
        except Exception as e:
            print(f"[Pipeline Error] Live cycle failed: {e}. Running rebuild over existing data...")
            rebuild_pipeline()


if __name__ == "__main__":
    main()
