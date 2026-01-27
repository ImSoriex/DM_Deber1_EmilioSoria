import base64
import requests
from datetime import datetime, timezone

from mage_ai.data_preparation.shared.secrets import get_secret_value

TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

def qbo_base_url(env: str) -> str:
    env = (env or "prod").lower()
    return "https://sandbox-quickbooks.api.intuit.com" if env == "sandbox" else "https://quickbooks.api.intuit.com"

def refresh_access_token(client_id: str, client_secret: str, refresh_token: str) -> dict:
    # Intuit requiere Basic Auth: base64(client_id:client_secret)
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("utf-8")
    headers = {
        "Authorization": f"Basic {basic}",
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    r = requests.post(TOKEN_URL, headers=headers, data=data, timeout=30)
    r.raise_for_status()
    return r.json()

def qbo_query(base_url: str, access_token: str, realm_id: str, query: str) -> dict:
    url = f"{base_url}/v3/company/{realm_id}/query"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/text",
    }
    r = requests.post(url, headers=headers, data=query, timeout=60)
    r.raise_for_status()
    return r.json()

# Si estás dentro de Mage, puedes dejar esto como @data_loader
def test_qbo_oauth():
    client_id = get_secret_value("qbo_client_id")
    client_secret = get_secret_value("qbo_client_secret")
    refresh_token = get_secret_value("qbo_refresh_token")
    realm_id = get_secret_value("qbo_realm_id")
    env = get_secret_value("qbo_env") or "sandbox"

    base_url = qbo_base_url(env)

    print("== QBO OAuth test ==")
    print(f"env: {env}")
    print(f"base_url: {base_url}")
    print(f"realm_id: {realm_id}")

    tokens = refresh_access_token(client_id, client_secret, refresh_token)
    access_token = tokens["access_token"]

    now_utc = datetime.now(timezone.utc).isoformat()
    print(f"[{now_utc}] ✅ Access token obtenido (len={len(access_token)})")

    # Query mínimo para confirmar permisos y conectividad
    query = "SELECT * FROM Customer MAXRESULTS 1"
    data = qbo_query(base_url, access_token, realm_id, query)

    customers = data.get("QueryResponse", {}).get("Customer", [])
    if isinstance(customers, dict):
        customers = [customers]

    count = len(customers)
    print(f"✅ QBO query OK. Customers devueltos: {count}")

    if count > 0:
        c0 = customers[0]
        print("Ejemplo de Customer:")
        print(f"  Id: {c0.get('Id')}")
        print(f"  DisplayName: {c0.get('DisplayName')}")
        print(f"  LastUpdatedTime: {c0.get('MetaData', {}).get('LastUpdatedTime')}")
    else:
        print("Ojo: la empresa sandbox no devolvió customers (puede pasar si está vacía).")

    return {"ok": True, "customers_returned": count}

# Ejecuta la prueba
test_qbo_oauth()
