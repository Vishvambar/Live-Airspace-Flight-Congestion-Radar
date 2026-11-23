"""
Gold Layer - Airspace Congestion Grids & Operational Anomaly Marts
Creates pre-aggregated, BI-ready tables in DuckDB:
1. gold_airspace_congestion: Spatial density grid heatmap and congestion hotspot detection
2. gold_country_summary: Aircraft distribution and flight dynamics breakdown by origin country
3. gold_flight_anomalies: High-priority operational alerts (emergency squawk, rapid climb/dive, low & fast)
"""

from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "airspace.duckdb"


def run_gold() -> dict:
    """Creates or updates all Gold Layer tables in DuckDB."""
    conn = duckdb.connect(str(DB_PATH))
    try:
        # Check silver_flights existence
        tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
        if "silver_flights" not in tables:
            raise RuntimeError("silver_flights table does not exist. Run transform_silver first.")

        # 1. Gold Airspace Congestion (Grid Heatmap & Hotspot Alerting)
        conn.execute(
            """
            CREATE OR REPLACE TABLE gold_airspace_congestion AS
            SELECT
                snapshot_time,
                grid_lat,
                grid_lon,
                COUNT(*) AS aircraft_count,
                ROUND(AVG(altitude_ft), 2) AS avg_altitude_ft,
                ROUND(AVG(speed_knots), 2) AS avg_speed_knots,
                CASE
                    WHEN COUNT(*) >= 5 THEN 'CONGESTION_HOTSPOT'
                    ELSE 'NORMAL'
                END AS congestion_status,
                (COUNT(*) >= 5) AS is_hotspot
            FROM silver_flights
            GROUP BY snapshot_time, grid_lat, grid_lon
            """
        )

        # 2. Gold Country Summary (Traffic Breakdown)
        conn.execute(
            """
            CREATE OR REPLACE TABLE gold_country_summary AS
            SELECT
                snapshot_time,
                origin_country,
                COUNT(*) AS total_aircraft,
                ROUND(AVG(CASE WHEN flight_phase = 'CRUISING' THEN speed_knots END), 2) AS avg_cruise_speed_knots,
                COUNT(CASE WHEN flight_phase = 'CLIMBING' THEN 1 END) AS climbing_count,
                COUNT(CASE WHEN flight_phase = 'DESCENDING' THEN 1 END) AS descending_count,
                COUNT(CASE WHEN flight_phase = 'CRUISING' THEN 1 END) AS cruising_count,
                COUNT(CASE WHEN flight_phase = 'ON_GROUND' THEN 1 END) AS on_ground_count
            FROM silver_flights
            GROUP BY snapshot_time, origin_country
            """
        )

        # 3. Gold Flight Anomalies (Operational Alerts)
        # Rules:
        # - Emergency Squawk: 7500 (hijack), 7600 (radio failure), 7700 (general emergency)
        # - Rapid Altitude Change: ABS(vertical_rate_fpm) > 3000
        # - Low & Fast: NOT on_ground AND altitude_ft < 10000 AND speed_knots > 280
        conn.execute(
            """
            CREATE OR REPLACE TABLE gold_flight_anomalies AS
            SELECT
                snapshot_time,
                icao24,
                callsign,
                origin_country,
                latitude,
                longitude,
                altitude_ft,
                speed_knots,
                vertical_rate_fpm,
                squawk,
                flight_phase,
                CASE
                    WHEN squawk IN ('7500', '7600', '7700') AND ABS(vertical_rate_fpm) > 3000 THEN 'EMERGENCY_SQUAWK & RAPID_ALT_CHANGE'
                    WHEN squawk IN ('7500', '7600', '7700') THEN 'EMERGENCY_SQUAWK (' || squawk || ')'
                    WHEN ABS(vertical_rate_fpm) > 3000 AND NOT on_ground AND altitude_ft < 10000 AND speed_knots > 280 THEN 'RAPID_DIVE & LOW_AND_FAST'
                    WHEN ABS(vertical_rate_fpm) > 3000 THEN 'RAPID_ALTITUDE_CHANGE'
                    WHEN NOT on_ground AND altitude_ft < 10000 AND speed_knots > 280 THEN 'LOW_AND_FAST'
                    ELSE 'OTHER_ANOMALY'
                END AS anomaly_type
            FROM silver_flights
            WHERE squawk IN ('7500', '7600', '7700')
               OR ABS(vertical_rate_fpm) > 3000
               OR (NOT on_ground AND altitude_ft < 10000 AND speed_knots > 280)
            """
        )

        counts = {
            "congestion_rows": conn.cursor().execute("SELECT COUNT(*) FROM gold_airspace_congestion").fetchall()[0][0],
            "hotspot_count": conn.cursor().execute("SELECT COUNT(*) FROM gold_airspace_congestion WHERE is_hotspot = true").fetchall()[0][0],
            "country_rows": conn.cursor().execute("SELECT COUNT(*) FROM gold_country_summary").fetchall()[0][0],
            "anomalies_detected": conn.cursor().execute("SELECT COUNT(*) FROM gold_flight_anomalies").fetchall()[0][0],
        }
        return counts
    finally:
        conn.close()


def main():
    print("Building Gold Layer Analytical Marts...")
    counts = run_gold()
    print(f"[Gold Success] Congestion Grid Cells: {counts['congestion_rows']} (Hotspots: {counts['hotspot_count']})")
    print(f"[Gold Success] Country Summaries: {counts['country_rows']}")
    print(f"[Gold Success] Flight Anomalies Flagged: {counts['anomalies_detected']}")

    # Verification queries
    conn = duckdb.connect(str(DB_PATH))
    try:
        print("\n--- Top Congestion Hotspots ---")
        df_hotspots = conn.execute(
            """
            SELECT snapshot_time, grid_lat, grid_lon, aircraft_count, avg_altitude_ft, avg_speed_knots
            FROM gold_airspace_congestion
            ORDER BY aircraft_count DESC
            LIMIT 5
            """
        ).df()
        print(df_hotspots.to_string(index=False))

        print("\n--- Operational Anomalies Sample ---")
        df_anomalies = conn.execute(
            """
            SELECT snapshot_time, callsign, origin_country, altitude_ft, speed_knots, vertical_rate_fpm, squawk, anomaly_type
            FROM gold_flight_anomalies
            LIMIT 5
            """
        ).df()
        print(df_anomalies.to_string(index=False) if len(df_anomalies) > 0 else "None detected in current snapshot")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
