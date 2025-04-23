#!/usr/bin/env python3
"""
Sync Avalanche L1 chains → Attio custom object “Blockchains”.

• Unique key  : chain_id  (attribute slug must be marked *Unique across object*)
• Other fields: name, status, rpc
• Every upserted record is added to list `ATTIO_LIST_ID`
• Chains without a chainId are skipped.
"""

from __future__ import annotations

import os
import sys
import time
import logging
from typing import Any, Final

import requests
from requests import Response, Session

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
ATTIO_TOKEN: Final[str]        = os.getenv("ATTIO_TOKEN", "")
ATTIO_OBJECT_ID: Final[str]    = os.getenv("ATTIO_OBJ", "")      # custom “Blockchains” object
ATTIO_LIST_ID: Final[str]      = os.getenv("ATTIO_LIST_ID", "")  # “Avalanche L1s” list
GLACIER_CHAINS_URL: Final[str] = "https://glacier-api.avax.network/v1/chains"
ATTIO_BASE_URL: Final[str]     = "https://api.attio.com/v2"
RATE_LIMIT_SECONDS: Final[float] = 0.2

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sync_l1s")

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
session = Session()
session.headers.update({"Authorization": f"Bearer {ATTIO_TOKEN}"})


def request_json(resp: Response) -> Any:
    """Raise for status, then return JSON."""
    resp.raise_for_status()
    return resp.json()


def status_from_chain(payload: dict) -> str:
    """Derive Mainnet / Testnet from the Glacier payload."""
    if "isMainnet" in payload:
        return "Mainnet" if payload["isMainnet"] else "Testnet"
    return "Testnet" if payload.get("isTestnet") else "Mainnet"


# -----------------------------------------------------------------------------
# Sync logic
# -----------------------------------------------------------------------------
def fetch_chains() -> list[dict]:
    """Download the full list of Avalanche chains."""
    log.info("Pulling chains from Glacier…")
    data = request_json(session.get(GLACIER_CHAINS_URL, timeout=15))
    chains = data.get("chains", [])
    log.info("Received %d chains", len(chains))
    return chains


def upsert_chain(chain: dict) -> str | None:
    """
    Upsert a single chain into Attio.
    Returns the Attio record ID (str) or None if missing.
    """
    chain_id = chain.get("chainId")
    if chain_id is None:
        log.warning("Skipping %s – no chainId in payload", chain.get("chainName", "(unnamed)"))
        return None

    values = {
        "chain_id": chain_id,               # unique key
        "name"    : chain.get("chainName"),
        "status"  : status_from_chain(chain),
        "rpc"     : chain.get("rpcUrl"),
    }

    resp = session.put(
        f"{ATTIO_BASE_URL}/objects/{ATTIO_OBJECT_ID}/records",
        params={"matching_attribute": "chain_id"},
        json={"data": {"values": values}},
        timeout=15,
    )
    body = request_json(resp).get("data", {})
    record_id: str | None = body.get("id") or body.get("record_id")

    if record_id:
        log.info("✓ Upserted chain_id %-8s → record %s", chain_id, record_id)
    else:
        log.warning("Upserted chain_id %s but response lacked a record ID", chain_id)

    return record_id


def ensure_on_list(record_id: str) -> None:
    """Add the record to the Avalanche L1s list (ignore duplicate errors)."""
    try:
        session.post(
            f"{ATTIO_BASE_URL}/lists/{ATTIO_LIST_ID}/entries",
            json={"data": {"record_id": record_id}},
            timeout=10,
        ).raise_for_status()
    except requests.HTTPError as exc:
        if exc.response.status_code == 409:  # already on list
            return
        raise


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> None:
    # Basic env-var sanity
    if not (ATTIO_TOKEN and ATTIO_OBJECT_ID and ATTIO_LIST_ID):
        log.critical("ATTIO_TOKEN / ATTIO_OBJ / ATTIO_LIST_ID env vars are required")
        sys.exit(1)

    chains = fetch_chains()

    upserted, skipped = 0, 0
    for chain in chains:
        try:
            record_id = upsert_chain(chain)
            if record_id:
                upserted += 1
                ensure_on_list(record_id)
            else:
                skipped += 1
        except Exception as exc:  # pylint: disable=broad-except
            log.error("⚠ Failed to process chain %s: %s", chain.get("chainId"), exc)
            skipped += 1
        finally:
            time.sleep(RATE_LIMIT_SECONDS)

    log.info("Done – %d upserted, %d skipped", upserted, skipped)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Interrupted by user – exiting")
