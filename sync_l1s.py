import os
import asyncio
import aiohttp
import logging
from typing import List, Dict, Any, Set, Optional

# --- Configuration ---
# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables
ATTIO_TOKEN: Optional[str] = os.environ.get('ATTIO_TOKEN')
ATTIO_OBJ_SLUG: Optional[str] = os.environ.get('ATTIO_OBJ') # e.g., "companies" or "people"
ATTIO_LIST_ID: Optional[str] = os.environ.get('ATTIO_LIST_ID') # e.g., "01HXXXX..."

# API Endpoints
ATTIO_API_BASE_URL: str = "https://api.attio.com/v2"
GLACIER_API_URL: str = "https://glacier-api.avax.network/v1/chains"

# Script behavior
CONCURRENCY_LIMIT: int = 10  # Max concurrent API requests to Attio
MAX_RETRIES: int = 5       # Max retries for an operation
INITIAL_RETRY_DELAY: float = 1.0 # Initial delay in seconds for retries

# --- Pre-computation and Validation ---
if not all([ATTIO_TOKEN, ATTIO_OBJ_SLUG, ATTIO_LIST_ID]):
    logging.error("Missing one or more environment variables: ATTIO_TOKEN, ATTIO_OBJ, ATTIO_LIST_ID")
    raise SystemExit("Configuration error: Missing critical environment variables.")

COMMON_HEADERS: Dict[str, str] = {
    "Authorization": f"Bearer {ATTIO_TOKEN}",
    "Content-Type": "application/json",
}

# Semaphore for controlling concurrency
api_semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)


async def _make_attio_request(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    action_description: str,
    chain_name_for_log: str,
    **kwargs: Any
) -> Optional[Dict[str, Any]]:
    """Helper function to make requests to Attio API with retries."""
    for attempt in range(MAX_RETRIES):
        try:
            async with api_semaphore: # Acquire semaphore before making the call
                async with session.request(method, url, headers=COMMON_HEADERS, **kwargs) as response:
                    if response.status == 429: # Rate limited
                        wait_time = (INITIAL_RETRY_DELAY * (2 ** attempt))
                        logging.warning(
                            f"Rate limited on {action_description} for '{chain_name_for_log}'. "
                            f"Attempt {attempt + 1}/{MAX_RETRIES}. Retrying in {wait_time:.2f}s…"
                        )
                        await asyncio.sleep(wait_time)
                        continue
                    
                    response_data = await response.json()
                    
                    if not (200 <= response.status < 300):
                        logging.error(
                            f"Error on {action_description} for '{chain_name_for_log}'. "
                            f"Status: {response.status}. Attempt {attempt + 1}/{MAX_RETRIES}. "
                            f"Response: {response_data}"
                        )
                        # For non-429 errors that are persistent, retry might not help,
                        # but we'll follow the MAX_RETRIES logic.
                        if attempt == MAX_RETRIES - 1:
                             logging.error(f"All retries failed for {action_description} on '{chain_name_for_log}'.")
                             return None # Failed after all retries
                        await asyncio.sleep(INITIAL_RETRY_DELAY * (2 ** attempt)) # Exponential backoff
                        continue

                    return response_data # Success
        
        except aiohttp.ClientError as e:
            logging.error(
                f"ClientError on {action_description} for '{chain_name_for_log}'. "
                f"Attempt {attempt + 1}/{MAX_RETRIES}. Error: {e}"
            )
        except asyncio.TimeoutError:
            logging.error(
                f"TimeoutError on {action_description} for '{chain_name_for_log}'. "
                f"Attempt {attempt + 1}/{MAX_RETRIES}."
            )
        except Exception as e: # Catch any other unexpected errors
            logging.error(
                f"Unexpected error on {action_description} for '{chain_name_for_log}'. "
                f"Attempt {attempt + 1}/{MAX_RETRIES}. Error: {type(e).__name__} - {e}"
            )
        
        if attempt < MAX_RETRIES - 1:
            wait_time = (INITIAL_RETRY_DELAY * (2 ** attempt))
            logging.info(f"Retrying {action_description} for '{chain_name_for_log}' in {wait_time:.2f}s...")
            await asyncio.sleep(wait_time)
        else:
            logging.error(f"All retries failed for {action_description} on '{chain_name_for_log}'.")
            return None # Failed after all retries
    return None


async def fetch_chains_from_glacier() -> List[Dict[str, Any]]:
    """Fetches chain data from the Glacier API."""
    logging.info(f"Fetching chain data from {GLACIER_API_URL}...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(GLACIER_API_URL, timeout=30) as response: # Added timeout
                response.raise_for_status()  # Raises an exception for 4XX/5XX status codes
                data = await response.json()
                chains = data.get("chains", [])
                logging.info(f"Successfully fetched {len(chains)} chains from Glacier API.")
                return chains
    except aiohttp.ClientError as e:
        logging.error(f"Error fetching chains from Glacier API: {e}")
    except asyncio.TimeoutError:
        logging.error(f"Timeout fetching chains from Glacier API.")
    except Exception as e:
        logging.error(f"Unexpected error fetching chains: {type(e).__name__} - {e}")
    return []


async def get_existing_list_entry_record_ids(session: aiohttp.ClientSession, list_id: str) -> Set[str]:
    """
    Fetches all parent_record_ids of entries currently in the specified Attio list.
    Handles pagination.
    """
    existing_record_ids: Set[str] = set()
    url = f"{ATTIO_API_BASE_URL}/lists/{list_id}/entries"
    params: Dict[str, Any] = {"limit": 100}  # Attio's max limit per page is 100

    logging.info(f"Fetching existing entries for list ID: {list_id}...")
    page_count = 0
    while True:
        page_count += 1
        logging.debug(f"Fetching page {page_count} of list entries (params: {params})...")
        
        response_data = await _make_attio_request(
            session,
            "GET",
            url,
            action_description=f"fetching page {page_count} of list entries",
            chain_name_for_log=f"ListID-{list_id}", # Generic name for list operation
            params=params
        )

        if not response_data:
            logging.error(f"Failed to fetch entries for list {list_id} after multiple retries. Aborting list member fetch.")
            break # Stop trying if a page fetch fails completely

        entries = response_data.get("data", [])
        for entry_data in entries:
            # Structure is data -> parent_record_id (object) -> record_id (string)
            parent_record_info = entry_data.get("parent_record_id")
            if isinstance(parent_record_info, dict):
                record_id = parent_record_info.get("record_id")
                if record_id:
                    existing_record_ids.add(record_id)
        
        next_page_cursor = response_data.get("result_set", {}).get("next_page")
        if next_page_cursor:
            params["after"] = next_page_cursor
        else:
            break  # No more pages

    logging.info(f"Found {len(existing_record_ids)} existing record_ids in list '{list_id}'.")
    return existing_record_ids


async def upsert_record_and_add_to_list(
    session: aiohttp.ClientSession,
    chain_data: Dict[str, Any],
    existing_list_member_ids: Set[str]
) -> None:
    """
    Upserts a record based on chain_data and adds it to the Attio list
    if it's not already present.
    """
    chain_name_original: str = chain_data.get("chainName", "Unknown Chain")
    is_testnet: bool = chain_data.get("isTestnet", False)
    chain_display_name: str = f"{chain_name_original}{' (Testnet)' if is_testnet else ''}"

    values_to_upsert: Dict[str, Any] = {
        "chain_id": str(chain_data.get("chainId", "")), # Matching attribute
        "name": chain_display_name,
        "rpc": chain_data.get("rpcUrl"),
        "status": "Testnet" if is_testnet else "Mainnet",
        "logo_url": chain_data.get("chainLogoUri"),
        # Add other attributes as needed, ensuring they exist on your Attio object
    }
    
    # Filter out any keys with None values, as Attio API might not like nulls for certain fields
    # unless explicitly supported. For simplicity, we remove them.
    values_to_upsert = {k: v for k, v in values_to_upsert.items() if v is not None}

    if not values_to_upsert.get("chain_id"):
        logging.warning(f"Skipping chain '{chain_display_name}' due to missing chainId.")
        return

    # 1. Upsert the record
    upsert_url = f"{ATTIO_API_BASE_URL}/objects/{ATTIO_OBJ_SLUG}/records"
    upsert_payload = {
        "data": {"values": values_to_upsert},
        "matching_attribute": "chain_id" # The unique key for matching
    }
    
    logging.debug(f"Attempting to upsert record for '{chain_display_name}' with chain_id: {values_to_upsert['chain_id']}")
    upsert_response_data = await _make_attio_request(
        session,
        "PUT",
        upsert_url,
        action_description="record upsert",
        chain_name_for_log=chain_display_name,
        json=upsert_payload
    )

    if not upsert_response_data or "data" not in upsert_response_data:
        logging.error(f"Failed to upsert record for '{chain_display_name}' or received invalid response.")
        return

    parent_record_id_obj = upsert_response_data.get("data", {}).get("id", {})
    parent_record_id: Optional[str] = None
    if isinstance(parent_record_id_obj, dict): # Attio returns id as {"workspace_id": "...", "record_id": "..."}
         parent_record_id = parent_record_id_obj.get("record_id")


    if not parent_record_id:
        logging.error(f"Failed to extract parent_record_id for '{chain_display_name}' from upsert response: {upsert_response_data}")
        return
    
    logging.info(f"Successfully upserted record for '{chain_display_name}'. Record ID: {parent_record_id}")

    # 2. Check if already in list
    if parent_record_id in existing_list_member_ids:
        logging.info(f"Record '{chain_display_name}' (ID: {parent_record_id}) is already in list '{ATTIO_LIST_ID}'. Skipping add.")
        return

    # 3. Add to list
    add_to_list_url = f"{ATTIO_API_BASE_URL}/lists/{ATTIO_LIST_ID}/entries"
    add_to_list_payload = {
        "data": {
            "parent_record_id": parent_record_id, # This should be the string ID
            "parent_object": ATTIO_OBJ_SLUG,
            "entry_values": {},  # Add any list-specific attributes here if needed
        }
    }
    
    logging.debug(f"Attempting to add record '{chain_display_name}' (ID: {parent_record_id}) to list '{ATTIO_LIST_ID}'.")
    add_response_data = await _make_attio_request(
        session,
        "POST",
        add_to_list_url,
        action_description="add to list",
        chain_name_for_log=chain_display_name,
        json=add_to_list_payload
    )

    if add_response_data:
        # Check for 409 Conflict specifically, as _make_attio_request treats it as an error to retry
        # However, if it makes it through, it means the API eventually returned something other than 429.
        # The original script checked for 409 here. If _make_attio_request handles retries for 409s
        # and eventually fails, add_response_data will be None.
        # If it succeeds (200/201), or if it's a 409 that _make_attio_request didn't filter out (unlikely with current setup),
        # we log success.
        # For a robust 409 check here, _make_attio_request would need to return the status or full response.
        # Given the current _make_attio_request, a 409 would likely result in add_response_data being None after retries.
        # So, we assume if add_response_data is not None, it was a success (200/201).
        logging.info(f"Successfully added record '{chain_display_name}' (ID: {parent_record_id}) to list '{ATTIO_LIST_ID}'.")
        existing_list_member_ids.add(parent_record_id) # Update our local set
    else:
        # This path is taken if _make_attio_request returned None after all retries.
        # This could be due to a persistent 409 (Conflict - already in list), or other errors.
        # To specifically know it was a 409, _make_attio_request would need to pass more info.
        # For now, we log a general failure to add.
        logging.error(f"Failed to add record '{chain_display_name}' (ID: {parent_record_id}) to list '{ATTIO_LIST_ID}' after retries.")


async def main() -> None:
    """Main orchestration function."""
    logging.info("Starting Attio sync process...")
    
    # Ensure ATTIO_LIST_ID is not None before proceeding
    if not ATTIO_LIST_ID:
        logging.critical("ATTIO_LIST_ID is not set. Cannot proceed.")
        return

    all_chains = await fetch_chains_from_glacier()
    if not all_chains:
        logging.warning("No chains fetched from Glacier API. Exiting.")
        return

    logging.info(f"Found {len(all_chains)} chains. Syncing with Attio object '{ATTIO_OBJ_SLUG}' and list '{ATTIO_LIST_ID}'...")

    # Use a single session for all API calls for connection pooling
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session: # Overall timeout for a request
        existing_list_members = await get_existing_list_entry_record_ids(session, ATTIO_LIST_ID)
        
        tasks = [
            upsert_record_and_add_to_list(session, chain_info, existing_list_members)
            for chain_info in all_chains
        ]
        await asyncio.gather(*tasks)

    logging.info("Attio sync process completed.")


if __name__ == "__main__":
    # For Windows, selector event loop might be needed if ProactorEventLoop causes issues.
    # if os.name == 'nt':
    #     asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
