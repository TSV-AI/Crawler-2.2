print("DEBUG: main.py script top level")
import asyncio

from crawl4ai import AsyncWebCrawler
from dotenv import load_dotenv

from config import BASE_URL, CSS_SELECTOR, REQUIRED_KEYS
from utils.data_utils import (
    save_venues_to_csv,
)
from utils.scraper_utils import (
    fetch_and_process_page,
    get_browser_config,
    get_llm_strategy,
)

load_dotenv()


async def crawl_venues():
    print("DEBUG: Entered crawl_venues()")
    """
    Main function to crawl venue data from the website.
    """
    # Initialize configurations
    browser_config = get_browser_config()
    llm_strategy = get_llm_strategy()
    session_id = "venue_crawl_session"

    # Initialize state variables
    page_number = 1
    all_venues = []
    seen_names = set()
    max_pages = 2  # Limit the number of pages to scrape

    # Start the web crawler context
    # https://docs.crawl4ai.com/api/async-webcrawler/#asyncwebcrawler
    async with AsyncWebCrawler(config=browser_config) as crawler:
        while page_number <= max_pages:
            current_url = f"{BASE_URL}?page={page_number}"
            print(f"Scraping URL: {current_url}")
            # Fetch and process data from the current page
            venues, no_results_found = await fetch_and_process_page(
                crawler,
                page_number,
                current_url,  # Pass current_url instead of BASE_URL
                CSS_SELECTOR,
                llm_strategy,
                session_id,
                REQUIRED_KEYS,
                seen_names,
            )

            if no_results_found:
                print("No more venues found. Ending crawl.")
                break  # Stop crawling when "No Results Found" message appears

            # The print for "No venues extracted from page" is now in fetch_and_process_page
            # Save the collected venues to a CSV file
                if all_venues:
                    # Define the mount path you configured in Railway
                    mount_path = "/data"  # <--- MAKE SURE THIS MATCHES YOUR RAILWAY VOLUME MOUNT PATH
                    filename = "complete_venues.csv"
                    full_path_to_csv = f"{mount_path}/{filename}" 
            
                    # Call your save function with the full path
                    save_venues_to_csv(all_venues, full_path_to_csv)
                    # Update the print statement to show the correct path
                    print(f"Saved {len(all_venues)} venues to '{full_path_to_csv}'.")
                else:
                    print("No venues were found during the crawl.")
            
                # Display usage statistics for the LLM strategy
                llm_strategy.show_usage()
            
            
            async def main():
                """
                Entry point of the script.
                """
                await crawl_venues()
            
            
            if __name__ == "__main__":
                asyncio.run(main())
