from __future__ import annotations
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from src.utils.config import PostgresConfig

#   Connect to the PostgreSQL database
def get_conn(cfg: PostgresConfig):
    # Connect to the PostgreSQL database
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        database=cfg.db,
        user=cfg.user,
        password=cfg.password
    )

def ensure_schema_and_table(conn, shcema: str, table: str, df: pd.DataFrame):
    # Create the schema if it doesn't exist
    cols_sql = ",\n".join([f'"{c}" TEXT' for c in df.columns])

    ddl = f'''
    create schema if not exists "{shcema}";
    create table if not exists "{shcema}".{table} (
        __ingested_at timestamptz default now(),
        {cols_sql}
    );
    '''
    # Execute the DDL
    with conn.cursor() as cur:
        cur.execute(ddl)

    conn.commit()
def append_rows(conn, shcema: str, table: str, df: pd.DataFrame, batch_size: int = 2000):
    # append the rows to the table

    if df.empty:
        return
    
    cols = list(df.columns)
    # convert from pandas to psycopg2 format
    values = df.astype(str).where(df.notna(), None).values.tolist()

    cols_Sql = ",".join([f'"{c}"' for c in cols])
    insert_Sql = f'insert into "{shcema}"."{table}"({cols_Sql}) values %s'

    with conn.cursor() as cur:
        for i in range(0, len(values), batch_size):
            chunk = values[i:i+batch_size]
            execute_values(cur, insert_Sql, chunk)
    conn.commit()