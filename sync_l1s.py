#!/usr/bin/env python3
"""
Sync Avalanche L1 chains → Attio Companies
• Unique key: domains (from officialSite)
• Extra fields: chain_id_6, rpc, status
• Chains with no officialSite are skipped without failing the workflow
"""

import os
import sys
import time
import requests
from urllib.parse import urlparse

ATTIO_BASE = "https://api.attio.com/v2"
HEADERS    = {"Authorization": f"Bearer {os.getenv('ATTIO_TOKEN')}"}

SKIPPED = 0
UPSERTED = 0

# ---------------------------------------------------------------------------
def root_domain(url: str | None) -> str | None:
    """Return bare domain or None."""
    if not url:
        return None
    # prepend scheme if missing so urlparse behaves
    parsed = urlparse(url if url.startswith(("http://", "https://")) else f"https://{url}")
    host = parsed.netloc.lower()
    return host[4:] if host.startswith("www.") else host or None


def status_from_payload(chain: dict) -> str:
    """Mainnet/Testnet logic."""
    if "isMainnet" in chain:
        return "Mainnet" if chain["isMainnet"] else "Testnet"
    return "Testnet" if chain.get("isTestnet") else "Mainnet"


# ---------------------------------------------------------------------------
def main() -> None:
    global SKIPPED, UPSERTED

    # 1. Download chain list
    try:
        chains = requests.get("https://glacier-api.avax.network/v1/chains")
        chains.raise_for_status()
        chains = chains.json().get("chains", [])
    except Exception as exc:
        print("🔥  Could not fetch Glacier chains:", exc, file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(chains)} chains – syncing to Attio")

    # 2. Process each chain
    for c in chains:
        domain = root_domain(c.get("officialSite"))
        if not domain:
            SKIPPED += 1
            print(f"⏭  {c.get('chainName','(no name)')} – skipped (no domain)")
            continue   # skip, do NOT fail

        values = {
            "domains":    [domain],
            "name":       c.get("chainName"),
            "chain_id_6": c.get("chainId"),
            "rpc":        c.get("rpcUrl"),
            "status":     status_from_payload(c),
        }

        try:
            # 2a. Upsert company via domains
            put = requests.put(
                f"{ATTIO_BASE}/objects/{os.getenv('ATTIO_OBJ')}/records",
                params={"matching_attribute": "domains"},
                json={"data": {"values": values}},
                headers=HEADERS,
            )
            put.raise_for_status()
            record_id = put.json()["data"]["record_id"]
            UPSERTED += 1
            print(f"✓  Upserted {domain}  → record {record_id}")

            # 2b. Ensure record is on list
            try:
                requests.post(
                    f"{ATTIO_BASE}/lists/{os.getenv('ATTIO_LIST_ID')}/entries",
                    json={"data": {"record_id": record_id}},
                    headers=HEADERS,
                ).raise_for_status()
            except requests.HTTPError as e:
                if e.response.status_code != 409:  # ignore duplicate entry
                    raise

        except Exception as exc:
            # Log the problem but keep looping so workflow finishes
            print(f"⚠️  Failed to upsert {domain}: {exc}", file=sys.stderr)

        time.sleep(0.2)   # polite rate-limit

    # -----------------------------------------------------------------------
    print(f"\nDone. Upserted {UPSERTED} chains, skipped {SKIPPED} (no domain).")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user")
