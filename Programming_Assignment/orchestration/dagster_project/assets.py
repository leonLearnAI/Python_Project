from __future__ import annotations
import psycopg2
from dagster import asset

from src.utils.config import PostgresConfig
from src.ingestion.run_ingestion import run_ingestion
from src.analysis.export_sql_outputs import run_export_outputs

def _get_conn():
    cfg = PostgresConfig()
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        database=cfg.db,
        user=cfg.user,
        password=cfg.password,
    )
# 1. Ingestion assets: raw files -> staging tables
@asset
def ingest_to_postgres(context) -> None:
    # load raw data from csv and json file into Postgres staging schema
    context.log.info("Running ingestion: from raw data to database")
    run_ingestion()
    context.log.info("Ingestion complete")

# 2. Create analytics views assets: staging tables -> analytics views
@asset(deps=[ingest_to_postgres])
def create_analytics_views(context) -> None:
    # create analytics views in Postgres schema
    context.log.info("Creating analytics views")
    sql = """
    CREATE SCHEMA IF NOT EXISTS analytics;

    CREATE OR REPLACE VIEW analytics.traffic_flow AS
    SELECT
      site,
      day,
      NULLIF(date, '')::date                AS date,
      NULLIF(start_time, '')::time          AS start_time,
      NULLIF(end_time, '')::time            AS end_time,
      NULLIF(flow, '')::numeric             AS flow,
      NULLIF(flow_pc, '')::numeric          AS flow_pc,
      NULLIF(cong, '')::numeric             AS cong,
      NULLIF(cong_pc, '')::numeric          AS cong_pc,
      NULLIF(dsat, '')::numeric             AS dsat,
      NULLIF(dsat_pc, '')::numeric          AS dsat_pc,
      "ObjectId"                            AS object_id,
      __ingested_at
    FROM staging.traffic_flow_sdcc_2023_h1;

    CREATE OR REPLACE VIEW analytics.crash_drivers AS
    SELECT
      "Report Number"                          AS report_number,
      "Local Case Number"                      AS local_case_number,
      "Agency Name"                            AS agency_name,
      "ACRS Report Type"                       AS acrs_report_type,
      NULLIF("Crash Date/Time",'')::timestamp  AS crash_datetime,
      "Route Type"                             AS route_type,
      "Road Name"                              AS road_name,
      "Cross-Street Name"                      AS cross_street_name,
      "Off-Road Description"                   AS off_road_description,
      "Municipality"                           AS municipality,
      "Collision Type"                         AS collision_type,
      "Weather"                                AS weather,
      "Surface Condition"                      AS surface_condition,
      "Light"                                  AS light,
      "Traffic Control"                        AS traffic_control,
      "Driver Substance Abuse"                 AS driver_substance_abuse,
      "Driver Distracted By"                   AS driver_distracted_by,
      "Driver At Fault"                        AS driver_at_fault,
      "Injury Severity"                        AS injury_severity,
      "Drivers License State"                  AS drivers_license_state,
      "Person ID"                              AS person_id,
      "Vehicle ID"                             AS vehicle_id,
      NULLIF("Speed Limit",'')::int            AS speed_limit,
      "Vehicle Year"                           AS vehicle_year,
      "Vehicle Make"                           AS vehicle_make,
      "Vehicle Model"                          AS vehicle_model,
      NULLIF("Latitude",'')::numeric           AS latitude,
      NULLIF("Longitude",'')::numeric          AS longitude,
      "Location"                               AS location,
      __ingested_at
    FROM staging.crash_drivers_montgomery_md;

    CREATE OR REPLACE VIEW analytics.crash_incidents AS
    SELECT
      NULLIF(tamainid,'')::bigint              AS tamainid,
      location_description,
      NULLIF(lat2,'')::numeric                 AS lat2,
      NULLIF(lon2,'')::numeric                 AS lon2,
      NULLIF(lat,'')::numeric                  AS lat,
      NULLIF(lon,'')::numeric                  AS lon,
      NULLIF(crash_date,'')::timestamptz       AS crash_date,
      NULLIF(ta_date,'')::date                 AS ta_date,
      ta_time,
      weather,
      lightcond,
      rdcondition,
      rdsurface,
      trafcontrl,
      rdfeature,
      rdcharacter,
      rdclass,
      rdconfigur,
      contributing_factor,
      vehicle_type,
      vehicle1, vehicle2, vehicle3, vehicle4, vehicle5,
      NULLIF(fatality,'')::int                 AS fatality,
      fatalities,
      injuries,
      NULLIF(possblinj,'')::int                AS possible_injury,
      NULLIF(numpassengers,'')::int            AS num_passengers,
      NULLIF(numpedestrians,'')::int           AS num_pedestrians,
      year,
      month,
      __ingested_at
    FROM staging.crash_incidents_cary_nc;
    """

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()
    context.log.info("Analytics views created")

@asset(deps=[create_analytics_views])
def export_sql_outputs(context) -> None:
    # export SQL outputs to CSV files
    context.log.info("Exporting SQL outputs to CSV files")
    run_export_outputs()
    context.log.info("Export complete")