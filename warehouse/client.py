"""Tiny read-only query helper shared by the dashboard and the notebook.

WAREHOUSE=postgres (default, local docker) or WAREHOUSE=snowflake. Returns pandas
DataFrames with lower-case column names so callers don't care which one is behind it.
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def load_env() -> None:
    env = ROOT / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


load_env()
WAREHOUSE = os.getenv("WAREHOUSE", "postgres").lower()
SCHEMA = "analytics"


def _connect():
    if WAREHOUSE == "snowflake":
        import snowflake.connector
        return snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.getenv("SNOWFLAKE_USER", "FINTECH_DBT"),
            private_key_file=str(ROOT / os.getenv("SNOWFLAKE_DBT_PRIVATE_KEY_PATH", "keys/dbt_key.p8")),
            role=os.getenv("SNOWFLAKE_ROLE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"), database=os.getenv("SNOWFLAKE_DATABASE"),
            schema="ANALYTICS",
        )
    import psycopg2
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"), port=int(os.getenv("PGPORT", "5433")),
        user=os.getenv("PGUSER", "fintech"), password=os.getenv("PGPASSWORD", "fintech"),
        dbname=os.getenv("PGDATABASE", "fintech"),
    )


def query(sql: str) -> pd.DataFrame:
    """Run a SELECT. Use `{schema}` in SQL for the analytics schema."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(sql.format(schema=SCHEMA))
        cols = [d[0].lower() for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        conn.close()
