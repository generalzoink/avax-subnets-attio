#!/usr/bin/env python3
"""
Sync Avalanche L1 chains → custom Attio object “Blockchains”
• Unique key: chain_id  (slug: chain_id)   ← mark this attribute “Unique across object” in Attio
• Extra fields: name, status, rpc
"""

import os
import sys
import time
import requests

ATTIO_BASE = "https://api.attio.com/v2"
HEADERS    = {"Authorization": f"Bearer {os.getenv('ATTIO_TOKEN')}"}

UPSERTED = 0
SKIPPED  = 0

# ---------------------------------------------------------------------------
def status_from_payload(chain: dict) -> str:
    """Return 'Mainnet' or 'Testnet'."""
    if "isMainnet" in chain:
        return "Mainnet" if chain["isMainnet"] else "Testnet"
    return "Testnet" if chain.get("isTestnet") else "Mainnet"


# ---------------------------------------------------------------------------
def main() -> None:
    global UPSERTED, SKIPPED

    # 1. Fetch every chain from Glacier
    try:
        resp = requests.get("https://glacier-api.avax.network/v1/chains")
        resp.raise_for_status()
        chains = resp.json().get("chains", [])
    except Exception as exc:
        print("🔥  Could not fetch Glacier chains:", exc, file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(chains)} chains – syncing to Attio")

    # 2. Upsert each chain
    for c in chains:
        chain_id = c.get("chainId")
        if chain_id is None:
            SKIPPED += 1
            print(f"⏭  {c.get('chainName','(no name)')} – skipped (no chainId)")
            continue

        values = {
            "chain_id": chain_id,              # ← unique slug
            "name":     c.get("chainName"),
            "status":   status_from_payload(c),
            "rpc":      c.get("rpcUrl"),
        }

        try:
            # 2a. Upsert using chain_id as the matching attribute
            put = requests.put(
                f"{ATTIO_BASE}/objects/{os.getenv('ATTIO_OBJ')}/records",
                params={"matching_attribute": "chain_id"},
                json={"data": {"values": values}},
                headers=HEADERS,
            )
            put.raise_for_status()
            record_id = put.json()["data"]["record_id"]
            UPSERTED += 1
            print(f"✓  Upserted chain_id {chain_id}  → record {record_id}")

            # 2b. Ensure it sits on the “Avalanche L1s” list
            try:
                requests.post(
                    f"{ATTIO_BASE}/lists/{os.getenv('ATTIO_LIST_ID')}/entries",
                    json={"data": {"record_id": record_id}},
                    headers=HEADERS,
                ).raise_for_status()
            except requests.HTTPError as e:
                if e.response.status_code != 409:      # 409 = already on list
                    raise

        except Exception as exc:
            # Log but carry on so workflow doesn’t fail on a single bad record
            print(f"⚠️  Failed to upsert chain_id {chain_id}: {exc}", file=sys.stderr)

        time.sleep(0.2)   # respectful rate-limit

    print(f"\nDone. Upserted {UPSERTED} chains, skipped {SKIPPED} (no chainId).")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user")
