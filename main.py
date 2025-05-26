# main.py for Crawler-2.2
print("DEBUG: main.py script top level")
import asyncio
import os
import httpx # For sending webhooks
from dotenv import load_dotenv

# Your project's specific imports
from crawl4ai import AsyncWebCrawler # Assuming this is how you import it
from config import BASE_URL, CSS_SELECTOR, REQUIRED_KEYS #

# Make sure the path and function names match your utils file
from utils.data_utils import init_db_pool, save_venues_to_db, close_db_pool 
from utils.scraper_utils import ( #
    fetch_and_process_page,
    get_browser_config,
    get_llm_strategy,
)

load_dotenv()
print("DEBUG: Successfully called load_dotenv()")


async def send_completion_webhook(status: str, message: str, venues_count: int = 0):
    """
    Sends a webhook notification with the crawl status.
    """
    webhook_url = os.getenv("COMPLETION_WEBHOOK_URL") # Set this ENV VAR in Railway
    if not webhook_url:
        print("DEBUG: COMPLETION_WEBHOOK_URL not set in environment. Skipping webhook.")
        return

    payload = {
        "status": status,
        "message": message,
        "venues_processed_count": venues_count,
        "service_name": "Crawler-2.2" 
    }
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(webhook_url, json=payload, timeout=10.0)
            response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
            print(f"DEBUG: Webhook sent successfully to {webhook_url}. Status: {response.status_code}")
    except httpx.RequestError as e:
        print(f"DEBUG: Error sending webhook (RequestError): {e}")
    except httpx.HTTPStatusError as e:
        print(f"DEBUG: Error sending webhook (HTTPStatusError): {e.response.status_code} - {e.response.text}")
    except Exception as e:
        print(f"DEBUG: An unexpected error occurred while sending webhook: {e}")


async def crawl_venues():
    print("DEBUG: Entered crawl_venues()")
    
    # Initialize DB Pool (this will also create the table if it doesn't exist)
    # init_db_pool from data_utils should raise an error if connection fails, stopping execution.
    await init_db_pool() 
    
    # Access the pool from your data_utils to ensure it was initialized
    from utils import data_utils # Or db_utils if you named it that
    if not data_utils.DB_POOL: # Or db_utils.DB_POOL
        print("DEBUG: Database pool was not initialized successfully in crawl_venues. Aborting.")
        # Webhook for DB connection failure is handled in main_runner's except block
        raise ConnectionError("Database pool initialization failed.") # Raise error to be caught by main_runner

    print("DEBUG: crawl_venues - Initializing crawler configurations")
    browser_config = get_browser_config()
    llm_strategy = get_llm_strategy() # Make sure this is defined in your scraper_utils
    session_id = "venue_crawl_session"
    print("DEBUG: crawl_venues - Crawler configurations initialized")

    page_number = 1
    all_venues_collected = [] 
    seen_names = set()
    max_pages = 2  # Adjust as needed

    print("DEBUG: crawl_venues - Starting AsyncWebCrawler context")
    async with AsyncWebCrawler(config=browser_config) as crawler:
        print("DEBUG: crawl_venues - Inside AsyncWebCrawler context")
        while page_number <= max_pages:
            current_url = f"{BASE_URL}?page={page_number}"
            print(f"Scraping URL: {current_url}")
            
            venues_from_page, no_results_found = await fetch_and_process_page(
                crawler, page_number, current_url, CSS_SELECTOR,
                llm_strategy, session_id, REQUIRED_KEYS, seen_names
            )

            if no_results_found:
                print(f"No more results found indication on page {page_number}. Ending crawl.")
                break
            
            if not venues_from_page: # If page processed but no valid venues extracted
                 print(f"No complete or non-duplicate venues found on page {page_number} after processing. Checking next page if within max_pages.")
                 # Decide if you want to stop or continue if a page yields nothing
                 # For now, it continues to the next page if max_pages not reached.
            
            all_venues_collected.extend(venues_from_page)
            if venues_from_page: # Only print if new venues were actually added
                print(f"Found {len(venues_from_page)} complete and non-duplicate venues on page {page_number}.")
            
            page_number += 1

            if page_number <= max_pages: # Only sleep if there's a next page to fetch
                print("DEBUG: crawl_venues - Pausing between requests")
                await asyncio.sleep(2)
    # End of while loop & async with crawler block
    print(f"DEBUG: crawl_venues - Finished scraping loop. Total pages attempted: {page_number-1}")

    venues_saved_count = 0
    if all_venues_collected:
        print(f"DEBUG: crawl_venues - Attempting to save {len(all_venues_collected)} venues to database.")
        venues_saved_count = await save_venues_to_db(all_venues_collected) 
        # save_venues_to_db from data_utils should print its own success/attempt message
    else:
        print("No venues were collected during the crawl to save to database.")

    # Display usage statistics for the LLM strategy
    print("DEBUG: crawl_venues - Showing LLM usage")
    if 'llm_strategy' in locals() and hasattr(llm_strategy, 'show_usage'):
        llm_strategy.show_usage()
    else:
        print("DEBUG: llm_strategy not available for usage summary.")
    print("DEBUG: Exiting crawl_venues()")
    return venues_saved_count # Return the count of venues processed for saving


async def main_runner(): 
    print("DEBUG: Entered main_runner()")
    venues_processed = 0
    status_message = "Crawl completed."
    crawl_status = "success"
    try:
        venues_processed = await crawl_venues()
    except ConnectionError as e: # Catch specific DB connection error from crawl_venues
        print(f"DEBUG: main_runner - Database connection error: {e}")
        status_message = f"Crawl failed: Database connection error - {e}"
        crawl_status = "failure"
    except Exception as e:
        print(f"DEBUG: main_runner - An unexpected error occurred during crawl_venues: {e}")
        import traceback
        traceback.print_exc()
        status_message = f"Crawl failed: An unexpected error occurred - {e}"
        crawl_status = "failure"
        # venues_processed will remain 0 or its last value if error happened mid-save
    finally:
        print("DEBUG: main_runner - Ensuring database pool is closed.")
        await close_db_pool() 
        print(f"DEBUG: main_runner - Sending completion webhook. Status: {crawl_status}, Venues: {venues_processed}")
        await send_completion_webhook(status=crawl_status, message=status_message, venues_count=venues_processed)
    print("DEBUG: Exited main_runner()")


if __name__ == "__main__":
    print("DEBUG: Script execution started in __main__ block")
    # Ensure your requirements.txt has 'asyncpg' and 'httpx'
    try:
        asyncio.run(main_runner())
        print("DEBUG: asyncio.run(main_runner()) completed")
    except Exception as e: # Should ideally be caught within main_runner's try/except/finally
        print(f"DEBUG: CRITICAL TOP LEVEL ERROR in __main__ execution (should have been caught in main_runner): {e}")
        import traceback
        traceback.print_exc()
        # Fallback webhook for critical error if main_runner's finally didn't run (e.g., during init_db_pool failure if not caught)
        asyncio.run(send_completion_webhook(status="critical_failure", message=f"Top-level script error: {e}"))
