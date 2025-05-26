print("DEBUG: main.py script top level")
import asyncio
from dotenv import load_dotenv

# Your existing imports for crawler
from crawl4ai import AsyncWebCrawler
from config import BASE_URL, CSS_SELECTOR, REQUIRED_KEYS #

# --- MODIFICATION: Import DB utility functions ---
# Make sure the path is correct (e.g., utils.data_utils or utils.db_utils)
# and that these functions exist in the file you just updated.
from utils.data_utils import init_db_pool, save_venues_to_db, close_db_pool 

from utils.scraper_utils import ( #
    fetch_and_process_page,
    get_browser_config,
    get_llm_strategy,
)

load_dotenv()
print("DEBUG: Successfully called load_dotenv()")

async def crawl_venues():
    print("DEBUG: Entered crawl_venues()")
    
    # --- MODIFICATION: Initialize DB Pool (this will also create the table) ---
    # This needs to be called before any database operations.
    # The init_db_pool function should handle exceptions and print errors if connection fails.
    # If it raises an exception, the script might stop here, which is intended if DB is unavailable.
    await init_db_pool()
    
    # Access the pool from your data_utils/db_utils module to check if it initialized
    # This assumes DB_POOL is a global variable in data_utils/db_utils as in the example I provided.
    from utils import data_utils # Or db_utils
    if not data_utils.DB_POOL: # Or db_utils.DB_POOL
        print("DEBUG: Database pool was not initialized successfully in crawl_venues. Aborting.")
        return # Exit if DB pool isn't there

    # Initialize configurations for crawling
    print("DEBUG: crawl_venues - Initializing crawler configurations")
    browser_config = get_browser_config()
    llm_strategy = get_llm_strategy()
    session_id = "venue_crawl_session"
    print("DEBUG: crawl_venues - Crawler configurations initialized")

    page_number = 1
    all_venues = [] # Keep collecting venues in memory first
    seen_names = set()
    max_pages = 2  # Limit the number of pages to scrape

    print("DEBUG: crawl_venues - Starting AsyncWebCrawler context")
    async with AsyncWebCrawler(config=browser_config) as crawler:
        print("DEBUG: crawl_venues - Inside AsyncWebCrawler context")
        while page_number <= max_pages:
            current_url = f"{BASE_URL}?page={page_number}"
            print(f"Scraping URL: {current_url}")
            
            venues, no_results_found = await fetch_and_process_page(
                crawler, page_number, current_url, CSS_SELECTOR,
                llm_strategy, session_id, REQUIRED_KEYS, seen_names
            )

            if no_results_found:
                print("No more venues found. Ending crawl.")
                break
            
            if not venues:
                 print(f"No complete or non-duplicate venues found on page {page_number} from fetch_and_process_page, stopping.")
                 break
            
            all_venues.extend(venues)
            print(f"Found {len(venues)} complete and non-duplicate venues on page {page_number}.")
            page_number += 1

            print("DEBUG: crawl_venues - Pausing between requests")
            await asyncio.sleep(2)
    # End of while loop & async with crawler block
    print(f"DEBUG: crawl_venues - Finished scraping loop. Total pages attempted: {page_number-1}")

    # --- MODIFICATION: Save collected venues to the database ---
    if all_venues:
        print(f"DEBUG: crawl_venues - Attempting to save {len(all_venues)} venues to database.")
        await save_venues_to_db(all_venues) 
        # The save_venues_to_db function should print its own success/attempt message.
    else:
        print("No venues were found during the crawl to save to database.")

    # Display usage statistics for the LLM strategy
    print("DEBUG: crawl_venues - Showing LLM usage")
    if 'llm_strategy' in locals() and hasattr(llm_strategy, 'show_usage'):
        llm_strategy.show_usage()
    else:
        print("DEBUG: llm_strategy not available for usage summary.")
    print("DEBUG: Exiting crawl_venues()")


async def main_runner(): # Renamed to avoid confusion if you have another 'main'
    print("DEBUG: Entered main_runner()")
    try:
        await crawl_venues()
    finally:
        # --- MODIFICATION: Ensure database pool is closed ---
        print("DEBUG: main_runner - Ensuring database pool is closed.")
        await close_db_pool() 
    print("DEBUG: Exited main_runner()")


if __name__ == "__main__":
    print("DEBUG: Script execution started in __main__ block")
    # Ensure your requirements.txt has 'asyncpg'
    try:
        asyncio.run(main_runner())
        print("DEBUG: asyncio.run(main_runner()) completed")
    except Exception as e:
        print(f"DEBUG: CRITICAL TOP LEVEL ERROR in __main__ execution: {e}")
        import traceback
        traceback.print_exc()
