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
            # So we only break here if no_results_found is True or venues is empty
            if not venues and not no_results_found:
                print(f"No complete or non-duplicate venues found on page {page_number}, stopping.")
                break


            # Add the venues from this page to the total list
            all_venues.extend(venues)
            print(f"Found {len(venues)} complete and non-duplicate venues on page {page_number}.")
            page_number += 1  # Move to the next page

            # Pause between requests to be polite and avoid rate limits
            await asyncio.sleep(2)  # Adjust sleep time as needed
        else:
            if page_number > max_pages:
                print(f"Finished scraping {max_pages} pages as requested.")

    # Save the collected venues to a CSV file
    if all_venues:
        save_venues_to_csv(all_venues, "complete_venues.csv")
        print(f"Saved {len(all_venues)} venues to 'complete_venues.csv'.")
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
