import json
from datetime import datetime, timezone

import pandas as pd

if "transformer" not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    Input (desde Extract):
      - id
      - payload
      - extract_window_start_utc
      - extract_window_end_utc
      - page_number
      - page_size
      - request_payload

    Output (para RAW Load):
      - id
      - payload_json
      - request_payload_json
      - ingested_at_utc
      - extract_window_start_utc
      - extract_window_end_utc
      - page_number
      - page_size
    """
    if df is None or df.empty:
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

    df = df.copy()

    # id obligatorio
    df = df[df["id"].notna()]
    df["id"] = df["id"].astype(str)

    # timestamps de ingesta (UTC)
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    df["ingested_at_utc"] = ingested_at

    # JSON listo para Postgres jsonb
    df["payload_json"] = df["payload"].apply(lambda x: json.dumps(x, ensure_ascii=False))
    df["request_payload_json"] = df["request_payload"].apply(lambda x: json.dumps(x, ensure_ascii=False))

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

    #print(type(out))
    #print(out.columns)
    return out
