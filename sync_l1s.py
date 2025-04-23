from urllib.parse import urlparse
import os, requests, time

ATTIO_BASE  = "https://api.attio.com/v2"
HEADERS     = {"Authorization": f"Bearer {os.environ['ATTIO_TOKEN']}"}

chains = requests.get("https://glacier-api.avax.network/v1/chains").json()["chains"]

for c in chains:
    # -----------------------------------------------------------
    # 1. derive the root domain from `officialSite`
    site  = c.get("officialSite")            # may be None on some chains
    domain = urlparse(site).netloc.lower().lstrip("www.") if site else None

    values = {
        "external_id":  str(c["chainId"]),               # still useful as stable ID
        "name":         c["chainName"],
        "chain_id_6":   c["chainId"],
        "rpc":          c["rpcUrl"],
        "status":       "Mainnet" if not c["isTestnet"] else "Testnet",
    }

    if domain:
        # Attio’s `domains` attribute expects an *array* of domains
        values["domains"] = [domain]

    # -----------------------------------------------------------
    # 2. Upsert Company on the natural key “domains” (preferred) or external_id fallback
    matching = "domains" if domain else "external_id"

    put = requests.put(
        f"{ATTIO_BASE}/objects/{os.environ['ATTIO_OBJ']}/records",
        params={"matching_attribute": matching},
        json = {"data": {"values": values}},
        headers = HEADERS,
    )
    put.raise_for_status()
    record_id = put.json()["data"]["record_id"]

    # -----------------------------------------------------------
    # 3. Ensure it’s on your “Avalanche L1s” list
    try:
        requests.post(
            f"{ATTIO_BASE}/lists/{os.environ['ATTIO_LIST_ID']}/entries",
            json={"data": {"record_id": record_id}},
            headers=HEADERS,
        ).raise_for_status()
    except requests.HTTPError as e:
        # 409 = already in list; anything else we still want to see
        if e.response.status_code != 409:
            raise

    time.sleep(0.2)          # gentle rate-limit
