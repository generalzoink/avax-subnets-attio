import os
import time
import requests

# Base URLs and headers
ATTIO_BASE = "https://api.attio.com/v2"
HEADERS = {"Authorization": f"Bearer {os.environ['ATTIO_TOKEN']}"}

# Fetch all Avalanche chains
resp = requests.get("https://glacier-api.avax.network/v1/chains")
resp.raise_for_status()
payload = resp.json()
# Pull out the array under "chains"
nets = payload.get("chains", [])

print(f"Found {len(nets)} chains, syncing into Attio…")

for n in nets:
    # Build the upsert payload
    body = {
        "external_id": str(n["chainId"]),
        "name":        n.get("chainName"),
        "attributes": {
            "Chain ID": n.get("chainId"),
            "RPC":      n.get("rpcUrl"),
            "Status":   "Mainnet" if n.get("isMainnet") else "Testnet",
        }
    }

    # 1) Upsert record (match on external_id)
    put = requests.put(
        f"{ATTIO_BASE}/objects/{os.environ['ATTIO_OBJ']}/records",
        params={"matching_attribute": "external_id"},
        json=body,
        headers=HEADERS
    )
    print("→ PUT", body["external_id"], "status:", put.status_code, put.text)
    put.raise_for_status()
    record_id = put.json()["record_id"]

    # 2) Add the record to your list
    post = requests.post(
        f"{ATTIO_BASE}/lists/{os.environ['ATTIO_LIST_ID']}/entries",
        json={"record_id": record_id},
        headers=HEADERS
    )
    print("→ POST entry", record_id, "status:", post.status_code, post.text)
    post.raise_for_status()

    # Rate-limit to avoid hammering the API
    time.sleep(0.2)
