import os, requests, time
ATTIO = "https://api.attio.com/v2"
HEAD  = {"Authorization": f"Bearer {os.environ['940191a34faae94b961302f41ab965989573bc613fff7a609834441467babd80']}"}

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
    rec = requests.put(f"{ATTIO}/objects/{os.environ['companies']}/records",
                       json=body, headers=HEAD).json()["record_id"]
    requests.post(f"{ATTIO}/lists/{os.environ['9a23136a-5390-4c8f-b7f7-4cd3f9267a00']}/entries",
                  json={"record_id": rec}, headers=HEAD)
    time.sleep(0.2)
