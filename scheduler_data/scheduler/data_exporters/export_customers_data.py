from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path

import pandas as pd
from psycopg2.extras import execute_values

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.qb_customers (
    id TEXT PRIMARY KEY,
    payload JSONB NOT NULL,
    ingested_at_utc TIMESTAMPTZ NOT NULL,
    extract_window_start_utc TIMESTAMPTZ NOT NULL,
    extract_window_end_utc TIMESTAMPTZ NOT NULL,
    page_number INTEGER NOT NULL,
    page_size INTEGER NOT NULL,
    request_payload JSONB NOT NULL
);
"""

UPSERT_SQL = """
INSERT INTO raw.qb_customers (
    id,
    payload,
    ingested_at_utc,
    extract_window_start_utc,
    extract_window_end_utc,
    page_number,
    page_size,
    request_payload
)
VALUES %s
ON CONFLICT (id) DO UPDATE SET
    payload = EXCLUDED.payload,
    ingested_at_utc = EXCLUDED.ingested_at_utc,
    extract_window_start_utc = EXCLUDED.extract_window_start_utc,
    extract_window_end_utc = EXCLUDED.extract_window_end_utc,
    page_number = EXCLUDED.page_number,
    page_size = EXCLUDED.page_size,
    request_payload = EXCLUDED.request_payload;
"""


@data_exporter
def export_data_to_postgres(df: pd.DataFrame, **kwargs) -> dict:
    """
    Espera un DF desde Transform con columnas:
      - id
      - payload_json
      - ingested_at_utc
      - extract_window_start_utc
      - extract_window_end_utc
      - page_number
      - page_size
      - request_payload_json
    """
    if df is None or df.empty:
        return {"rows_loaded": 0}

    schema_name = "raw"
    table_name = "qb_customers"

    config_path = path.join(get_repo_path(), "io_config.yaml")
    config_profile = "default"

    rows = list(
        df[
            [
                "id",
                "payload_json",
                "ingested_at_utc",
                "extract_window_start_utc",
                "extract_window_end_utc",
                "page_number",
                "page_size",
                "request_payload_json",
            ]
        ].itertuples(index=False, name=None)
    )

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        # Acceso al cursor/conn de psycopg2 por debajo
        conn = loader.conn
        with conn.cursor() as cur:
            cur.execute(DDL)

            template = (
                "("
                "%s, "
                "%s::jsonb, "
                "%s::timestamptz, "
                "%s::timestamptz, "
                "%s::timestamptz, "
                "%s, "
                "%s, "
                "%s::jsonb"
                ")"
            )

            execute_values(cur, UPSERT_SQL, rows, template=template, page_size=1000)

        conn.commit()

    return {"rows_loaded": len(rows), "target": f"{schema_name}.{table_name}"}
