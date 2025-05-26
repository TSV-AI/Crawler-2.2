print("DEBUG: main.py script top level") # You saw this one

try:
    import asyncio
    print("DEBUG: Successfully imported asyncio")

    from crawl4ai import AsyncWebCrawler
    print("DEBUG: Successfully imported AsyncWebCrawler from crawl4ai")

    from dotenv import load_dotenv
    print("DEBUG: Successfully imported load_dotenv from dotenv")

    from config import BASE_URL, CSS_SELECTOR, REQUIRED_KEYS
    print("DEBUG: Successfully imported from config")

    from utils.data_utils import save_venues_to_csv
    print("DEBUG: Successfully imported from utils.data_utils")

    from utils.scraper_utils import (
        fetch_and_process_page,
        get_browser_config,
        get_llm_strategy,
    )
    print("DEBUG: Successfully imported from utils.scraper_utils")

    print("DEBUG: About to call load_dotenv()")
    load_dotenv()
    print("DEBUG: Successfully called load_dotenv()")

except Exception as e:
    print(f"DEBUG: CRITICAL ERROR DURING IMPORT OR LOAD_DOTENV: {e}")
    import traceback
    traceback.print_exc()
    # Exit or raise to ensure logs show the error if in a tricky spot
    raise

# --- Your original code continues below, but with corrected indentation ---
# --- I've fixed a major indentation issue here for main() and if __name__ ---

async def crawl_venues():
    print("DEBUG: Entered crawl_venues()")
    """
    Main function to crawl venue data from the website.
    """
    # Initialize configurations
    print("DEBUG: crawl_venues - Initializing configurations")
    browser_config = get_browser_config()
    llm_strategy = get_llm_strategy()
    session_id = "venue_crawl_session"
    print("DEBUG: crawl_venues - Configurations initialized")

    # Initialize state variables
    page_number = 1
    all_venues = []
    seen_names = set()
    max_pages = 2  # Limit the number of pages to scrape

    # Start the web crawler context
    print("DEBUG: crawl_venues - Starting AsyncWebCrawler context")
    async with AsyncWebCrawler(config=browser_config) as crawler:
        print("DEBUG: crawl_venues - Inside AsyncWebCrawler context")
        while page_number <= max_pages:
            current_url = f"{BASE_URL}?page={page_number}"
            print(f"Scraping URL: {current_url}")
            # Fetch and process data from the current page
            venues, no_results_found = await fetch_and_process_page(
                crawler,
                page_number,
                current_url,
                CSS_SELECTOR,
                llm_strategy,
                session_id,
                REQUIRED_KEYS,
                seen_names,
            )

            if no_results_found:
                print("No more venues found. Ending crawl.")
                break

            if not venues: # This condition might have been intended differently based on your original partial snippet
                print(f"No complete or non-duplicate venues found on page {page_number} (after processing).")
                # If no_results_found was false, but venues list is empty, consider if you should break or just note it.
                # The original snippet had the save logic inside the "if no_results_found:" which was incorrect.
                # Original snippet: if not venues and not no_results_found: print(...) break
                # This means if venues *were* found (not (not venues)) OR if no_results_found *was* true, it wouldn't break.
                # Let's assume for now if venues is empty after processing, we might want to continue or log differently.
                # For safety, if no venues and no "no_results_found" flag, let's check original intent:
                # The line was: if not venues and not no_results_found: print(...) break
                # This means: if (we didn't get any venues from fetch_and_process_page for *this page*) AND (the no_results_found flag wasn't set by check_no_results)
                # then stop. This implies fetch_and_process_page could return an empty list even if the page isn't the "no results" page.
                # This seems okay as a condition to stop if a page yields nothing useful.
                if not venues: # Simplified: if fetch_and_process_page returned no venues for this page (and not due to "No Results Found" page)
                     print(f"No complete or non-duplicate venues found on page {page_number} from fetch_and_process_page, stopping.")
                     break


            # Add the venues from this page to the total list
            all_venues.extend(venues)
            print(f"Found {len(venues)} complete and non-duplicate venues on page {page_number}.")
            page_number += 1

            # Pause between requests
            print("DEBUG: crawl_venues - Pausing between requests")
            await asyncio.sleep(2)
        # End of while loop
    # End of async with crawler

    print(f"DEBUG: crawl_venues - Finished scraping loop. Total pages attempted: {page_number-1}")

    # Save the collected venues to a CSV file - This is now correctly outside the loop
    if all_venues:
        mount_path = "/data"
        filename = "complete_venues.csv"
        full_path_to_csv = f"{mount_path}/{filename}"
        print(f"DEBUG: crawl_venues - Attempting to save to {full_path_to_csv}")
        save_venues_to_csv(all_venues, full_path_to_csv)
        # The save_venues_to_csv function already prints a save message.
        # print(f"Saved {len(all_venues)} venues to '{full_path_to_csv}'.")
    else:
        print("No venues were found during the crawl.")

    # Display usage statistics for the LLM strategy
    print("DEBUG: crawl_venues - Showing LLM usage")
    llm_strategy.show_usage()
    print("DEBUG: Exiting crawl_venues()")


async def main():
    print("DEBUG: Entered main()")
    await crawl_venues()
    print("DEBUG: Exited main()")


if __name__ == "__main__":
    print("DEBUG: Script execution started in __main__ block")
    try:
        asyncio.run(main())
        print("DEBUG: asyncio.run(main()) completed")
    except Exception as e:
        print(f"DEBUG: CRITICAL TOP LEVEL ERROR in __main__ execution: {e}")
        import traceback
        traceback.print_exc()
