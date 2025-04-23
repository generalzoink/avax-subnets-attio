import os, requests, time
ATTIO = "https://api.attio.com/v2"
HEAD  = {"Authorization": f"Bearer {os.environ['ATTIO_TOKEN']}"}

nets = requests.get("https://glacier-api.avax.network/v1/networks").json()["items"]
for n in nets:
    body = {
      "external_id": n["slug"],
      "name": n["name"],
      "attributes": {
        "Chain ID": n["chainId"],
        "RPC": n["rpcUrl"],
        "Status": "Mainnet" if n["isMainnet"] else "Testnet"
      }
    }
    rec = requests.put(f"{ATTIO}/objects/{os.environ['ATTIO_OBJ']}/records",
                       json=body, headers=HEAD).json()["record_id"]
    requests.post(f"{ATTIO}/lists/{os.environ['ATTIO_LIST_ID']}/entries",
                  json={"record_id": rec}, headers=HEAD)
    time.sleep(0.2)
