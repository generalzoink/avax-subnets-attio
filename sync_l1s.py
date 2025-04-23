#!/usr/bin/env python3
"""
Sync Avalanche L1 chains → custom Attio object “Blockchains”
• Unique key: chain_id  (slug: chain_id)
• Extra fields: name, status, rpc
• Robust to missing IDs in the response
"""

import os, sys, time, requests

ATTIO_BASE = "https://api.attio.com/v2"
HEADERS    = {"Authorization": f"Bearer {os.getenv('ATTIO_TOKEN')}"}

def status_from_payload(c: dict) -> str:
    return "Mainnet" if c.get("isMainnet", not c.get("isTestnet", False)) else "Testnet"

def main() -> None:
    try:
        chains = requests.get("https://glacier-api.avax.network/v1/chains")
        chains.raise_for_status()
        chains = chains.json().get("chains", [])
    except Exception as exc:
        print("🔥  Could not fetch Glacier chains:", exc, file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(chains)} chains – syncing to Attio")

    for c in chains:
        chain_id = c.get("chainId")
        if chain_id is None:
            print(f"⏭  Skipping (no chainId)  {c.get('chainName')}")
            continue

        values = {
            "chain_id": chain_id,
            "name":     c.get("chainName"),
            "status":   status_from_payload(c),
            "rpc":      c.get("rpcUrl"),
        }

        try:
            put = requests.put(
                f"{ATTIO_BASE}/objects/{os.getenv('ATTIO_OBJ')}/records",
                params={"matching_attribute": "chain_id"},
                json={"data": {"values": values}},
                headers=HEADERS,
            )
            put.raise_for_status()
            body = put.json().get("data", {})
            record_id = body.get("record_id") or body.get("id")  # ← handle both shapes

            if record_id:
                print(f"✓  Upserted chain_id {chain_id} → record {record_id}")
                try:
                    requests.post(
                        f"{ATTIO_BASE}/lists/{os.getenv('ATTIO_LIST_ID')}/entries",
                        json={"data": {"record_id": record_id}},
                        headers=HEADERS,
                    ).raise_for_status()
                except requests.HTTPError as e:
                    if e.response.status_code != 409:  # ignore “already in list”
                        raise
            else:
                print(f"⚠️  Upserted chain_id {chain_id} but no record ID returned – not added to list")

        except Exception as exc:
            print(f"⚠️  Failed to upsert chain_id {chain_id}: {exc}", file=sys.stderr)

        time.sleep(0.2)

if __name__ == "__main__":
    main()
