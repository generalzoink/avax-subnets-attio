import os, requests, time, json

ATTIO = "https://api.attio.com/v2"
HEAD  = {"Authorization": f"Bearer {os.environ['ATTIO_TOKEN']}"}

# 1) fetch & debug-dump
resp = requests.get("https://glacier-api.avax.network/v1/networks")
resp.raise_for_status()
payload = resp.json()
print("⛰ Glacier payload:", json.dumps(payload, indent=2))

# 2) pick the right key
if isinstance(payload, dict):
    if   "items"    in payload: nets = payload["items"]
    elif "networks" in payload: nets = payload["networks"]
    elif "data"     in payload: nets = payload["data"]
    else:
        raise RuntimeError(f"Unexpected keys: {list(payload)}")
elif isinstance(payload, list):
    nets = payload
else:
    raise RuntimeError(f"Unexpected type: {type(payload)}")

# 3) loop over nets
for n in nets:
    body = {
      "external_id": n["slug"],
      "name": n["name"],
      "attributes": {
        "Chain ID": n["chainId"],
        "RPC":      n["rpcUrl"],
        "Status":   "Mainnet" if n["isMainnet"] else "Testnet",
      }
    }
    rec = requests.put(
      f"{ATTIO}/objects/{os.environ['ATTIO_OBJ']}/records",
      json=body, headers=HEAD
    ).json()["record_id"]
    requests.post(
      f"{ATTIO}/lists/{os.environ['ATTIO_LIST_ID']}/entries",
      json={"record_id": rec}, headers=HEAD
    )
    time.sleep(0.2)
