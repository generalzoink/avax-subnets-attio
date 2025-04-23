import os, requests, time, json

ATTIO = "https://api.attio.com/v2"
HEAD  = {"Authorization": f"Bearer {os.environ['ATTIO_TOKEN']}"}

# 1) Fetch chains
resp = requests.get("https://glacier-api.avax.network/v1/chains")
resp.raise_for_status()
payload = resp.json()
nets    = payload.get("items") or payload.get("data") or []

# 2) Loop + upsert
for n in nets:
    body = {
      "external_id": n.get("slug") or n.get("id"),
      "name":        n.get("name"),
      "attributes": {
        "Chain ID": n.get("chainId") or n.get("chainID"),
        "RPC":      n.get("rpcUrl"),
        "Status":   "Mainnet" if n.get("isMainnet") else "Testnet",
      }
    }

    # PUT record
    put = requests.put(
      f"{ATTIO}/objects/{os.environ['ATTIO_OBJ']}/records",
      json=body, headers=HEAD
    )
    print("PUT", body["external_id"], "→", put.status_code, put.text)
    put.raise_for_status()
    rec_id = put.json()["record_id"]

    # POST list entry
    post = requests.post(
      f"{ATTIO}/lists/{os.environ['ATTIO_LIST_ID']}/entries",
      json={"record_id": rec_id}, headers=HEAD
    )
    print("POST entry", rec_id, "→", post.status_code, post.text)
    post.raise_for_status()

    time.sleep(0.2)
