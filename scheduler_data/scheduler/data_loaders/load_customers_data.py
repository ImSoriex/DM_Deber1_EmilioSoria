import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Tuple

import pandas as pd
import requests
from mage_ai.data_preparation.shared.secrets import get_secret_value

if "data_loader" not in globals():
    from mage_ai.data_preparation.decorators import data_loader


TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"


def get_base_url() -> str:
    env = get_secret_value("qbo_env")
    if str(env).lower() == "sandbox":
        return "https://sandbox-quickbooks.api.intuit.com"
    return "https://quickbooks.api.intuit.com"


def get_access_token() -> str:
    client_id = get_secret_value("qbo_client_id")
    client_secret = get_secret_value("qbo_client_secret")
    refresh_token = get_secret_value("qbo_refresh_token")

    response = requests.post(
        TOKEN_URL,
        auth=requests.auth.HTTPBasicAuth(client_id, client_secret),
        data={"grant_type": "refresh_token", "refresh_token": refresh_token},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def request_get_with_retries(
    url: str,
    headers: Dict[str, str],
    params: Dict[str, Any],
    max_retries: int = 5
) -> Dict[str, Any]:
    for attempt in range(max_retries):
        r = requests.get(url, headers=headers, params=params, timeout=60)

        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(2 ** attempt)
            continue

        r.raise_for_status()
        return r.json()

    raise RuntimeError("QuickBooks request falló tras varios reintentos.")


def format_qb_datetime(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def split_date_range(start_utc: datetime, end_utc: datetime, chunk_days: int) -> List[Tuple[datetime, datetime]]:
    windows = []
    current = start_utc
    while current < end_utc:
        nxt = min(current + timedelta(days=chunk_days), end_utc)
        windows.append((current, nxt))
        current = nxt
    return windows


def fetch_customers_window(
    realm_id: str,
    base_url: str,
    access_token: str,
    start_dt: datetime,
    end_dt: datetime,
    minor_version: int = 75,
    page_size: int = 1000,
) -> List[Dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/v3/company/{realm_id}/query"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

    start_str = format_qb_datetime(start_dt)
    end_str = format_qb_datetime(end_dt)

    start_position = 1
    page_number = 1
    rows: List[Dict[str, Any]] = []

    while True:
        query = (
            "SELECT * FROM Customer "
            f"WHERE MetaData.LastUpdatedTime >= '{start_str}' "
            f"AND MetaData.LastUpdatedTime < '{end_str}' "
            f"STARTPOSITION {start_position} MAXRESULTS {page_size}"
        )

        params = {"query": query, "minorversion": minor_version}
        data = request_get_with_retries(url, headers, params)

        if "Fault" in data:
            break

        customers = data.get("QueryResponse", {}).get("Customer", [])
        if not customers:
            break

        request_payload = {"query": query, "minorversion": minor_version}

        for c in customers:
            rows.append(
                {
                    "id": str(c.get("Id")) if c.get("Id") is not None else None,
                    "payload": c,
                    "extract_window_start_utc": start_str,
                    "extract_window_end_utc": end_str,
                    "page_number": page_number,
                    "page_size": page_size,
                    "request_payload": request_payload,
                }
            )

        if len(customers) < page_size:
            break

        start_position += page_size
        page_number += 1
        time.sleep(0.3)

    return rows


@data_loader
def load_data(*args, **kwargs) -> pd.DataFrame:
    fecha_inicio = kwargs.get("fecha_inicio")
    fecha_fin = kwargs.get("fecha_fin")
    if not fecha_inicio or not fecha_fin:
        raise ValueError("Debes pasar fecha_inicio y fecha_fin, ej: '2024-01-01T00:00:00Z'.")

    chunk_days = int(kwargs.get("chunk_days", 30))

    start_utc = datetime.fromisoformat(fecha_inicio.replace("Z", "+00:00")).astimezone(timezone.utc)
    end_utc = datetime.fromisoformat(fecha_fin.replace("Z", "+00:00")).astimezone(timezone.utc)

    realm_id = get_secret_value("qbo_realm_id")
    base_url = get_base_url()
    access_token = get_access_token()

    windows = split_date_range(start_utc, end_utc, chunk_days)

    all_rows: List[Dict[str, Any]] = []
    for w_start, w_end in windows:
        all_rows.extend(
            fetch_customers_window(
                realm_id=realm_id,
                base_url=base_url,
                access_token=access_token,
                start_dt=w_start,
                end_dt=w_end,
            )
        )

    df = pd.DataFrame(all_rows)
    #print(df.shape)
    #print(df.columns)
    #print(df.head(5))
    return df
