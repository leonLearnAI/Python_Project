# src/analysis/export_sql_outputs.py
from __future__ import annotations

from pathlib import Path
import pandas as pd

from src.utils.config import PostgresConfig, get_paths
from src.storage.postgres_io import get_conn


def export_query(conn, sql: str, output_file: Path) -> None:
    """
    Execute a SQL query and export the result to a CSV file.
    """
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df = pd.read_sql_query(sql, conn)
    df.to_csv(output_file, index=False)
    print(f"[OK] Exported {len(df)} rows -> {output_file}")


def main() -> None:
    """
    Task 8: Export SQL outputs (analysis results) to results/outputs/*.csv
    """
    cfg = PostgresConfig()
    conn = get_conn(cfg)

    # Debug: confirm we are connected to the expected database/schema
    print("[DEBUG] Connected to:", cfg.host, cfg.port, cfg.db, cfg.user)
    print(pd.read_sql_query("SELECT current_database() AS db, current_schema() AS schema;", conn))
    print(pd.read_sql_query("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='analytics' AND table_name='traffic_flow'
        ORDER BY ordinal_position;
    """, conn))

    paths = get_paths()
    output_dir = paths["results_outputs"]

    queries: dict[str, str] = {
        # Traffic flow outputs
        "traffic_top10_sites.csv": """
            SELECT
              site,
              SUM(flow) AS total_flow
            FROM analytics.traffic_flow
            WHERE flow IS NOT NULL
            GROUP BY site
            ORDER BY total_flow DESC
            LIMIT 10;
        """,
        "traffic_hourly_pattern.csv": """
            SELECT
              EXTRACT(HOUR FROM start_time) AS hour,
              AVG(flow) AS avg_flow,
              AVG(cong) AS avg_congestion,
              AVG(dsat) AS avg_saturation
            FROM analytics.traffic_flow
            WHERE start_time IS NOT NULL
            GROUP BY 1
            ORDER BY 1;
        """,

        # Drivers outputs (normalized categories)
        "drivers_injury_severity_distribution.csv": """
            SELECT
              COALESCE(NULLIF(UPPER(TRIM(injury_severity)), ''), 'UNKNOWN') AS injury_severity,
              COUNT(*) AS cnt
            FROM analytics.crash_drivers
            GROUP BY 1
            ORDER BY cnt DESC;
        """,
        "drivers_at_fault_distribution.csv": """
            SELECT
              COALESCE(NULLIF(UPPER(TRIM(driver_at_fault)), ''), 'UNKNOWN') AS driver_at_fault,
              COUNT(*) AS cnt
            FROM analytics.crash_drivers
            GROUP BY 1
            ORDER BY cnt DESC;
        """,
        "drivers_top10_municipalities.csv": """
            SELECT
              municipality,
              COUNT(*) AS cnt
            FROM analytics.crash_drivers
            WHERE municipality IS NOT NULL AND TRIM(municipality) <> ''
            GROUP BY municipality
            ORDER BY cnt DESC
            LIMIT 10;
        """,

        # Cary incidents outputs
        "cary_weather_counts.csv": """
            SELECT
              weather,
              COUNT(*) AS cnt
            FROM analytics.crash_incidents
            GROUP BY weather
            ORDER BY cnt DESC;
        """,
        "cary_monthly_counts.csv": """
            SELECT
              DATE_TRUNC('month', crash_date) AS month,
              COUNT(*) AS cnt
            FROM analytics.crash_incidents
            WHERE crash_date IS NOT NULL
            GROUP BY 1
            ORDER BY 1;
        """,
        "cary_fatal_injury_summary.csv": """
            SELECT
              SUM(CASE WHEN fatality IS NOT NULL AND fatality > 0 THEN 1 ELSE 0 END) AS fatal_cases,
              SUM(CASE WHEN possible_injury IS NOT NULL AND possible_injury > 0 THEN 1 ELSE 0 END) AS possible_injury_cases,
              COUNT(*) AS total_cases
            FROM analytics.crash_incidents;
        """,
    }

    for file_name, sql in queries.items():
        export_query(conn, sql, output_dir / file_name)

    conn.close()
    print("[DONE] All outputs exported.")

# dagster pipeline to export the SQL outputs to CSV files
def run_export_outputs() -> None:
    main()