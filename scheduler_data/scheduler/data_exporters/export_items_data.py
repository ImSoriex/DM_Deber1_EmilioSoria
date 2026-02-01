from datetime import datetime, timezone
from os import path

import pandas as pd
from psycopg2.extras import execute_values

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts} UTC] {msg}")


DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.qb_items (
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
INSERT INTO raw.qb_items (
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
    if df is None or df.empty:
        log("Load: DataFrame vacío. No se carga nada.")
        return {"rows_loaded": 0}

    required_cols = [
        "id",
        "payload_json",
        "ingested_at_utc",
        "extract_window_start_utc",
        "extract_window_end_utc",
        "page_number",
        "page_size",
        "request_payload_json",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Load: faltan columnas requeridas: {missing}")

    log(f"Load: filas recibidas = {len(df)}")

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

    log("Load: abriendo conexión a Postgres...")

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        # Algunas versiones exponen conn como loader.conn, otras diferente:
        conn = getattr(loader, "conn", None) or getattr(loader, "_conn", None) or getattr(loader, "_connection", None)
        if conn is None:
            raise AttributeError("Load: no pude obtener la conexión desde el loader (conn/_conn/_connection).")

        log("Load: conexión abierta. Creando tabla si no existe...")
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

            log("Load: ejecutando upsert...")
            execute_values(cur, UPSERT_SQL, rows, template=template, page_size=1000)

        conn.commit()
        log("Load: commit realizado.")

    log("Load: terminado.")
    return {"rows_loaded": len(rows), "target": "raw.qb_items"}
