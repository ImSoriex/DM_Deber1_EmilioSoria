import json
from datetime import datetime, timezone

import pandas as pd

if "transformer" not in globals():
    from mage_ai.data_preparation.decorators import transformer


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts} UTC] {msg}")


@transformer
def transform(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    if df is None or df.empty:
        log("Transform: DataFrame vacío.")
        return pd.DataFrame(
            columns=[
                "id",
                "payload_json",
                "request_payload_json",
                "ingested_at_utc",
                "extract_window_start_utc",
                "extract_window_end_utc",
                "page_number",
                "page_size",
            ]
        )

    log(f"Transform: filas recibidas = {len(df)}")

    df = df.copy()

    # id obligatorio
    df = df[df["id"].notna()]
    df["id"] = df["id"].astype(str)

    # ingesta (UTC)
    df["ingested_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # JSON listo para jsonb
    df["payload_json"] = df["payload"].apply(
        lambda x: json.dumps(x if x is not None else {}, ensure_ascii=False)
    )
    df["request_payload_json"] = df["request_payload"].apply(
        lambda x: json.dumps(x if x is not None else {}, ensure_ascii=False)
    )

    # asegurar tipos
    df["page_number"] = pd.to_numeric(df["page_number"], errors="coerce").fillna(0).astype(int)
    df["page_size"] = pd.to_numeric(df["page_size"], errors="coerce").fillna(0).astype(int)

    out = df[
        [
            "id",
            "payload_json",
            "request_payload_json",
            "ingested_at_utc",
            "extract_window_start_utc",
            "extract_window_end_utc",
            "page_number",
            "page_size",
        ]
    ].reset_index(drop=True)

    log(f"Transform: filas de salida = {len(out)}")
    return out