# subnets-attio/sync_l1s.py  – only the parts that change are shown
import os, asyncio, aiohttp, json
ATTIO_BASE = "https://api.attio.com/v2"
GLACIER_URL = "https://glacier-api.avax.network/v1/chains"
HEADERS = {"Authorization": f"Bearer {os.environ['ATTIO_TOKEN']}"}
ATTIO_OBJ = os.environ['ATTIO_OBJ']
ATTIO_LIST_ID = os.environ['ATTIO_LIST_ID']
CONCURRENCY_LIMIT = 20
sem = asyncio.Semaphore(CONCURRENCY_LIMIT)

async def assert_list_entry(session: aiohttp.ClientSession, parent_record_id: str):
    """
    Idempotently create OR update the list‑entry whose parent is `parent_record_id`.
    If multiple matches already exist we keep the oldest and delete the extras,
    then retry once.
    """
    payload = {
        "data": {
            "parent_record_id": parent_record_id,
            "parent_object": ATTIO_OBJ,
            "entry_values": {}
        }
    }
    for attempt in range(5):                     # back‑off on 429s
        async with session.put(
            f"{ATTIO_BASE}/lists/{ATTIO_LIST_ID}/entries",
            json=payload, headers=HEADERS
        ) as resp:
            if resp.status in (200, 201):
                return                          # success (created or updated)
            if resp.status == 429:
                await asyncio.sleep(2 ** attempt)
                continue
            if resp.status == 400:
                body = await resp.json()
                if body.get("error_code") == "MULTIPLE_MATCH_RESULTS":
                    # More than one entry already exists – clean up duplicates
                    await dedupe_list_entries(session, parent_record_id)
                    continue                    # retry once after cleanup
            text = await resp.text()
            raise RuntimeError(f"List‑assert failed ({resp.status}): {text}")
            return

async def dedupe_list_entries(session, parent_record_id):
    """Delete all but the earliest entry whose parent is `parent_record_id`."""
    offset, limit, all_entries = 0, 500, []
    while True:
        async with session.post(
            f"{ATTIO_BASE}/lists/{ATTIO_LIST_ID}/entries/query",
            json={
                "filter": {"parent_record_id": parent_record_id},
                "sorts": [{"attribute": "created_at", "direction": "asc"}],
                "limit": limit, "offset": offset
            }, headers=HEADERS
        ) as resp:
            page = (await resp.json()).get("data", [])
            all_entries.extend(page)
            if len(page) < limit:
                break
            offset += limit
    # keep the first (oldest) entry, delete the rest
    for entry in all_entries[1:]:
        entry_id = entry["id"]["entry_id"]
        async with session.delete(
            f"{ATTIO_BASE}/lists/{ATTIO_LIST_ID}/entries/{entry_id}",
            headers=HEADERS
        ) as del_resp:
            if del_resp.status not in (200, 204):
                print(f"⚠️ couldn't delete dup entry {entry_id}: {del_resp.status}")

async def upsert_and_add_to_list(session, chain):
    async with sem:
        for attempt in range(5):
            try:
                # ---------- 1. build values ----------
                values = {
                    "chain_id": str(chain["chainId"]),
                    "name": f"{chain['chainName']}{' (Testnet)' if chain['isTestnet'] else ''}",
                    "rpc": chain.get("rpcUrl"),
                    "status": "Testnet" if chain["isTestnet"] else "Mainnet",
                    "logo_url": chain.get("chainLogoUri"),
                }

                # ---------- 2. upsert object ----------
                async with session.put(
                    f"{ATTIO_BASE}/objects/{ATTIO_OBJ}/records",
                    params={"matching_attribute": "chain_id"},
                    json={"data": {"values": values}},
                    headers=HEADERS,
                ) as put_resp:
                    if put_resp.status == 429:
                        await asyncio.sleep(2 ** attempt); continue
                    put_data = await put_resp.json()
                    parent_record_id = (
                        put_data.get("data", {})
                        .get("id", {})
                        .get("record_id")
                    )
                    if not parent_record_id:
                        print(f"⚠️ upsert failed for {values['name']}: {await put_resp.text()}")
                        return

                # ---------- 3. assert entry (idempotent) ----------
                await assert_list_entry(session, parent_record_id)
                print(f"✅ synced: {values['name']}")
                break

            except Exception as e:
                print(f"❌ Unexpected error for {chain.get('chainName')}: {e}")
                raise 
