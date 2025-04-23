import os, time, requests

ATTIO_BASE = "https://api.attio.com/v2"
HEADERS     = {"Authorization": f"Bearer {os.environ['ATTIO_TOKEN']}"}

# ------------------------------------------------------------------
# 1. Pull every chain from Glacier
chains = requests.get("https://glacier-api.avax.network/v1/chains").json().get("chains", [])
print(f"Found {len(chains)} chains, syncing into Attio…")

for c in chains:
    values = {
        "chain_id": str(c["chainId"]),                    # unique key we’ll match on
        "name"        : c.get("chainName"),
        "rpc"         : c.get("rpcUrl"),
        "status"      : "Mainnet" if c.get("isMainnet") else "Testnet",
    }

    # ------------------------------------------------------------------
    # 2. Upsert the Company record (match on external_id)
    put = requests.put(
        f"{ATTIO_BASE}/objects/{os.environ['ATTIO_OBJ']}/records",
        params={"matching_attribute": "chain_id"},
        json = {"data": {"values": values}},
        headers = HEADERS,
    )
    # ------------------------------------------------------------------
    # 3. Add / ensure the record is on your list
    #    (If you re-run the script this won’t error)
    try:
        requests.post(
            f"{ATTIO_BASE}/lists/{os.environ['ATTIO_LIST_ID']}/entries",
            json={"data": {"chain_id": "chainId"}},
            headers=HEADERS,
        ).raise_for_status()
        print("  ↳ added to list")
    except requests.HTTPError as e:
        if e.response.status_code != 409:     # 409 = already in list
            raise

    time.sleep(0.2)  # rate-limit
