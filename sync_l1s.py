import os
import asyncio
import aiohttp

ATTIO_BASE = "https://api.attio.com/v2"
GLACIER_URL = "https://glacier-api.avax.network/v1/chains"
HEADERS = {"Authorization": f"Bearer {os.environ['ATTIO_TOKEN']}"}
ATTIO_OBJ = os.environ['ATTIO_OBJ']
ATTIO_LIST_ID = os.environ['ATTIO_LIST_ID']
CONCURRENCY_LIMIT = 20  # Adjust depending on API rate limits

sem = asyncio.Semaphore(CONCURRENCY_LIMIT)

async def fetch_chains():
    async with aiohttp.ClientSession() as session:
        async with session.get(GLACIER_URL) as resp:
            data = await resp.json()
            return data.get("chains", [])

async def fetch_existing_ids(session):
    """Fetch existing list entry parent_record_ids."""
    existing = set()
    page = 1
    while True:
        async with session.get(
            f"{ATTIO_BASE}/lists/{ATTIO_LIST_ID}/entries",
            params={"page": page},
            headers=HEADERS,
        ) as resp:
            if resp.status != 200:
                print(f"❌ Error fetching existing entries: {resp.status}")
                break
            data = await resp.json()
            for entry in data.get("data", []):
                pid = entry.get("parent_record_id")
                if pid:
                    existing.add(pid)
            meta = data.get("meta", {})
            if meta.get("next_page"):
                page = meta["next_page"]
            else:
                break
    return existing


async def upsert_and_add_to_list(session, chain, existing_ids):
    async with sem:
        for attempt in range(5):  # max 5 retries
            try:
                chain_name = chain.get("chainName")
                is_testnet = chain.get("isTestnet")
                if is_testnet:
                    chain_name += " (Testnet)"

                logo_url = chain.get("chainLogoUri")
                    
                values = {
                    "chain_id": str(chain["chainId"]),
                    "name": chain_name,
                    "rpc": chain.get("rpcUrl"),
                    "status": "Testnet" if is_testnet else "Mainnet",
                    "logo_url": logo_url,
                }

                # Upsert the record
                async with session.put(
                    f"{ATTIO_BASE}/objects/{ATTIO_OBJ}/records",
                    params={"matching_attribute": "chain_id"},
                    json={"data": {"values": values}},
                    headers=HEADERS,
                ) as put_resp:
                    if put_resp.status == 429:
                        wait = 2 ** attempt
                        print(f"⚠️ Rate limited on upsert for {chain['chainName']}, retrying in {wait}s…")
                        await asyncio.sleep(wait)
                        continue
                    put_data = await put_resp.json()
                    # print(put_data)
                    parent_record_id = put_data.get("data", {}).get("id", {}).get("record_id")
                    # print(parent_record_id)
                    if not parent_record_id:
                        print(f"⚠️ Failed to upsert: {chain.get('chainName')}")
                        print(f"⚠️ Response: {await put_resp.text()}")
                        print(values)
                        print(chain)
                        return

                if parent_record_id in existing_ids:
                    print(f"↳ already synced: {chain_name}")
                    break

                # Add to list
                async with session.post(
                    f"{ATTIO_BASE}/lists/{ATTIO_LIST_ID}/entries",
                    json={
                        "data": {
                            "parent_record_id": parent_record_id,
                            "parent_object": ATTIO_OBJ,
                            "entry_values": {},
                        }
                    },
                    headers=HEADERS,
                ) as post_resp:
                    if post_resp.status == 429:
                        wait = 2 ** attempt
                        print(
                            f"⚠️ Rate limited on list entry for {chain['chainName']}, retrying in {wait}s…"
                        )
                        await asyncio.sleep(wait)
                        continue
                    elif post_resp.status in [200, 201, 409]:
                        if post_resp.status == 409:
                            print(f"↳ already in list: {chain['chainName']}")
                        else:
                            print(f"✅ added to list: {chain['chainName']}")
                        existing_ids.add(parent_record_id)
                    else:
                        text = await post_resp.text()
                        print(
                            f"❌ Error adding {chain['chainName']}: {post_resp.status} - {text}"
                        )
                break  # Exit retry loop if all went well
            except Exception as e:
                print(f"❌ Unexpected error for {chain['chainName']}: {e}")
                break

async def main():
    chains = await fetch_chains()
    print(f"Found {len(chains)} chains, syncing into Attio…")

    async with aiohttp.ClientSession() as session:
        existing_ids = await fetch_existing_ids(session)
        tasks = [upsert_and_add_to_list(session, c, existing_ids) for c in chains]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
