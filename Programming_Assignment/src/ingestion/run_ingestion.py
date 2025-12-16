from __future__ import annotations
from curses import raw
from os import read

from numpy import append
from pandas import read_json
from requests import get

from src.utils.config import PostgresConfig, get_paths
from src.ingestion.ingest_csv import readcsv
from src.storage.postgres_io import get_conn, ensure_schema_and_table, append_rows

# this method will be ingest the data into the database
def ingest_File(conn, schema: str, table: str, df):

    #  creat the schema and table if not exist
    ensure_schema_and_table(conn, schema, table, df)
    append_rows(conn, schema, table, df)
    print(f"[OK]{len(df)} Data Ingested into {schema}.{table}")

def main():

    # get the raw data directory path
    paths = get_paths()
    raw_dir = paths["data_raw"]

    # make sure the raw data directory exist
    traffic_csv = raw_dir / "traffic_flow_sdcc_2023_h1.csv"
    drivers_csv = raw_dir / "crash_drivers_montgomery_md.csv"
    crashes_json = raw_dir / "crash_incidents_cary_nc.json"
    # read the data from the csv and json files
    traffic_df = readcsv(traffic_csv)
    drivers_df = readcsv(drivers_csv)
    crashes_df = read_json(crashes_json)

    # connect to the database
    cfg = PostgresConfig()
    conn = get_conn(cfg)

    schema = "staging"
    # ingest the data into the database one by one
    ingest_File(conn, schema, "traffic_flow_sdcc_2023_h1", traffic_df)
    ingest_File(conn, schema, "crash_drivers_montgomery_md", drivers_df)
    ingest_File(conn, schema, "crash_incidents_cary_nc", crashes_df)

    conn.close()

if __name__ == "__main__":
    main()

# dagster pipeline to ingest the data into the database
def run_ingestion() -> None:
    main()