import os
import time
import requests

ATTIO = "https://api.attio.com/v2"
HEAD  = {"Authorization": f"Bearer {os.environ['ATTIO_TOKEN']}"}

# fetch the chains
resp = requests.get("https://glacier-api.avax.network/v1/chains")
resp.raise_for_status()
payload = resp.json()
nets = payload.get("chains") or []

print(f"Found {len(nets)} chains, syncing into Attio…")

for n in nets:
    body = {
      "external_id": n["chainId"],
      "name":        n.get("name"),
      "attributes": {
        "Chain ID": n.get("chainId"),
        "RPC":      n.get("rpcUrl"),
        "Status":   "Mainnet" if n.get("isMainnet") else "Testnet",
      }
    }

    # 1) Upsert record
    put = requests.put(
      f"{ATTIO}/objects/{os.environ['ATTIO_OBJ']}/records",
      json=body, headers=HEAD
    )
    print("→ PUT", body["external_id"], "status:", put.status_code, put.text)
    put.raise_for_status()
    record_id = put.json()["record_id"]

    # 2) Add to list
    post = requests.post(
      f"{ATTIO}/lists/{os.environ['ATTIO_LIST_ID']}/entries",
      json={"record_id": record_id}, headers=HEAD
    )
    print("→ POST entry", record_id, "status:", post.status_code, post.text)
    post.raise_for_status()

    time.sleep(0.2)
