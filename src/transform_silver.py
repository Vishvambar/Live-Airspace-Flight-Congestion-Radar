"""
Silver Layer - Data Cleaning, Aviation Unit Conversion & Data Quality Audit
Transforms raw ADS-B telemetry into standardized silver_flights
and records transformation metrics in dq_audit_log table.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "airspace.duckdb"


def init_silver_tables(conn: duckdb.DuckDBPyConnection):
    """Initializes silver_flights and dq_audit_log tables if they do not exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dq_audit_log (
            run_timestamp TIMESTAMP,
            snapshot_time BIGINT,
            raw_row_count BIGINT,
            null_geo_dropped BIGINT,
            duplicates_removed BIGINT,
            silver_row_count BIGINT,
            pass_rate_pct DOUBLE
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS silver_flights (
            icao24 VARCHAR,
            callsign VARCHAR,
            origin_country VARCHAR,
            time_position BIGINT,
            last_contact BIGINT,
            longitude DOUBLE,
            latitude DOUBLE,
            baro_altitude DOUBLE,
            on_ground BOOLEAN,
            velocity DOUBLE,
            true_track DOUBLE,
            vertical_rate DOUBLE,
            sensors VARCHAR,
            geo_altitude DOUBLE,
            squawk VARCHAR,
            spi BOOLEAN,
            position_source BIGINT,
            category BIGINT,
            snapshot_time BIGINT,
            ingested_at_utc VARCHAR,
            altitude_ft DOUBLE,
            speed_knots DOUBLE,
            vertical_rate_fpm DOUBLE,
            flight_phase VARCHAR,
            grid_lat DOUBLE,
            grid_lon DOUBLE
        );
        """
    )


def transform_snapshot(conn: duckdb.DuckDBPyConnection, snapshot_time: int) -> dict:
    """
    Cleans and standardizes raw telemetry for a single snapshot:
    1. String normalization on callsign
    2. Drops missing GPS coordinates (WHERE lat/lon IS NOT NULL)
    3. Idempotent deduplication via QUALIFY ROW_NUMBER()
    4. Metric to Aviation unit conversions (meters -> feet, m/s -> knots, m/s -> fpm)
    5. Feature engineering: flight_phase and spatial grid coordinates (grid_lat, grid_lon)
    6. Appends audit record to dq_audit_log
    """
    run_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # 1. Calculate raw row count
    cur = conn.cursor()
    raw_row_count = cur.execute(
        "SELECT COUNT(*) FROM bronze_states WHERE snapshot_time = ?",
        [snapshot_time],
    ).fetchall()[0][0]

    # 2. Count records dropped due to null lat/lon
    null_geo_dropped = cur.execute(
        """
        SELECT COUNT(*) FROM bronze_states
        WHERE snapshot_time = ? AND (latitude IS NULL OR longitude IS NULL)
        """,
        [snapshot_time],
    ).fetchall()[0][0]

    valid_geo_count = raw_row_count - null_geo_dropped

    # 3. Clean and insert into silver_flights idempotently (replace existing rows for this snapshot)
    cur.execute("DELETE FROM silver_flights WHERE snapshot_time = ?", [snapshot_time])

    insert_sql = """
    INSERT INTO silver_flights
    SELECT
        icao24,
        COALESCE(NULLIF(TRIM(callsign), ''), 'UNKNOWN') AS callsign,
        origin_country,
        time_position,
        last_contact,
        longitude,
        latitude,
        baro_altitude,
        on_ground,
        velocity,
        true_track,
        vertical_rate,
        sensors,
        geo_altitude,
        squawk,
        spi,
        position_source,
        category,
        snapshot_time,
        ingested_at_utc,
        ROUND(baro_altitude * 3.28084, 2) AS altitude_ft,
        ROUND(velocity * 1.94384, 2) AS speed_knots,
        ROUND(vertical_rate * 196.85, 2) AS vertical_rate_fpm,
        CASE
            WHEN on_ground THEN 'ON_GROUND'
            WHEN vertical_rate > 2.5 THEN 'CLIMBING'
            WHEN vertical_rate < -2.5 THEN 'DESCENDING'
            ELSE 'CRUISING'
        END AS flight_phase,
        ROUND(latitude, 1) AS grid_lat,
        ROUND(longitude, 1) AS grid_lon
    FROM bronze_states
    WHERE snapshot_time = ?
      AND latitude IS NOT NULL
      AND longitude IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY icao24, snapshot_time ORDER BY last_contact DESC) = 1
    """
    cur.execute(insert_sql, [snapshot_time])

    # 4. Count silver rows produced
    silver_row_count = cur.execute(
        "SELECT COUNT(*) FROM silver_flights WHERE snapshot_time = ?",
        [snapshot_time],
    ).fetchall()[0][0]

    # 5. Duplicates removed is difference between valid geo rows and deduped silver rows
    duplicates_removed = valid_geo_count - silver_row_count
    pass_rate_pct = round((silver_row_count * 100.0 / raw_row_count), 2) if raw_row_count > 0 else 100.0

    # 6. Insert audit row into dq_audit_log
    cur.execute("DELETE FROM dq_audit_log WHERE snapshot_time = ?", [snapshot_time])
    cur.execute(
        """
        INSERT INTO dq_audit_log (
            run_timestamp, snapshot_time, raw_row_count,
            null_geo_dropped, duplicates_removed, silver_row_count, pass_rate_pct
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_timestamp,
            snapshot_time,
            raw_row_count,
            null_geo_dropped,
            duplicates_removed,
            silver_row_count,
            pass_rate_pct,
        ],
    )

    metrics = {
        "snapshot_time": snapshot_time,
        "raw_row_count": raw_row_count,
        "null_geo_dropped": null_geo_dropped,
        "duplicates_removed": duplicates_removed,
        "silver_row_count": silver_row_count,
        "pass_rate_pct": pass_rate_pct,
    }
    return metrics


def run_silver(snapshot_time: Optional[int] = None) -> List[dict]:
    """
    Runs silver transformation.
    If snapshot_time is provided, transforms that specific snapshot.
    Otherwise, transforms all snapshots available in bronze_states.
    """
    conn = duckdb.connect(str(DB_PATH))
    try:
        init_silver_tables(conn)

        # Check bronze_states existence
        tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
        if "bronze_states" not in tables:
            print("[Silver Warning] bronze_states table does not exist in DuckDB.")
            return []

        if snapshot_time is not None:
            snapshots = [snapshot_time]
        else:
            snapshots = [
                row[0]
                for row in conn.execute(
                    "SELECT DISTINCT snapshot_time FROM bronze_states ORDER BY snapshot_time ASC"
                ).fetchall()
            ]

        results = []
        for snap in snapshots:
            metrics = transform_snapshot(conn, snap)
            results.append(metrics)
            print(
                f"[Silver Audit] Snapshot {snap}: "
                f"Raw={metrics['raw_row_count']}, "
                f"NullGeo={metrics['null_geo_dropped']}, "
                f"Dups={metrics['duplicates_removed']}, "
                f"Silver={metrics['silver_row_count']}, "
                f"PassRate={metrics['pass_rate_pct']}%"
            )

        return results
    finally:
        conn.close()


def main():
    print("Running Silver Layer Transformations & DQ Audit...")
    results = run_silver()
    if not results:
        print("No snapshots to transform.")
        return

    # Verification queries
    conn = duckdb.connect(str(DB_PATH))
    try:
        print("\n--- Recent DQ Audit Log ---")
        df_audit = conn.execute("SELECT * FROM dq_audit_log ORDER BY run_timestamp DESC LIMIT 5").df()
        print(df_audit.to_string(index=False))

        print("\n--- Silver Flight Phases Breakdown ---")
        df_phases = conn.execute(
            """
            SELECT flight_phase, COUNT(*) as aircraft_count
            FROM silver_flights
            GROUP BY 1
            ORDER BY 2 DESC
            """
        ).df()
        print(df_phases.to_string(index=False))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
