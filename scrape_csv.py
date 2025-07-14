# Import tools for scraping, validating, and file handling
import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
import concurrent.futures
import re
import os
from datetime import datetime
import pandas as pd
import csv


# Define a structure to ensure hotel ratings are valid
class HotelRating(BaseModel):
    name: str
    rating: float = Field(..., ge=0, le=5, description="Hotel star rating (0-5)")
    review_count: int = Field(..., ge=0, description="Number of reviews")

# Create a Google Maps search link for the hotel
def form_search_url(hotel_name: str) -> str:
    query = f"{hotel_name} hotel reviews".replace(" ", "+")
    return f"https://www.google.com/maps/search/{query}"

# Scrape Google Maps for hotel rating and review count
def scrape_hotel_rating(hotel_name: str) -> dict:
    start_time = time.time()
    """
    Scrape Google Maps for a hotel's star rating and number of reviews.
    Uses Selenium to load dynamic content and Pydantic to validate data.
    Returns a dictionary with name, rating, and review_count.
    Note: Scraping may violate Google Maps' terms; consider Google Business Profile API.
    """
    print(f"Starting scrape for {hotel_name} at {datetime.now().strftime('%H:%M:%S')}")
    url = form_search_url(hotel_name)
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-webgl")  # Disable WebGL to reduce GPU errors
    driver = webdriver.Chrome(options=options)
    try:
        driver.get(url)
        time.sleep(3)  # Wait for page load
        # Click the first hotel result to see details
        try:
            first_result = driver.find_element(By.CSS_SELECTOR, 'div[role="article"]')
            first_result.click()
            time.sleep(3)  # Increased to avoid CAPTCHAs
        except Exception:
            pass
        # Initialize variables
        rating = None
        review_count = None
        # Try to find rating
        try:
            elems = driver.find_elements(By.CSS_SELECTOR, 'div.F7nice span[aria-label*="stars"], span.MW4etd, [aria-label*="Rated"], [aria-label*="out of 5"]')
            for elem in elems:
                aria = elem.get_attribute('aria-label') or elem.text
                if aria:
                    match = re.search(r'(\d+\.\d+|\d+)(?=\s*(?:stars|out of 5|/5|rating|rated))', aria, re.IGNORECASE)
                    if match:
                        rating = float(match.group(1))
                        break
        except Exception:
            pass
        # Fallback: try meta tag or visible text
        if rating is None or rating == 0.0:
            try:
                meta_rating = driver.find_element(By.CSS_SELECTOR, 'meta[itemprop="ratingValue"]')
                rating = float(meta_rating.get_attribute('content'))
            except Exception:
                try:
                    rating_text = driver.find_element(By.CSS_SELECTOR, 'span.MW4etd, span[jsname="Te9Tpc"], .aMPvhf-fI6EEc-KVuj8d').text
                    rating = float(rating_text.split()[0])
                except Exception:
                    rating = 0.0
        # Try to find review count
        try:
            review_elem = driver.find_element(By.CSS_SELECTOR, 'span.OEwtMc, button[jsaction*="pane.rating.moreReviews"], span[aria-label*="reviews"]')
            review_text = review_elem.text
            match = re.search(r'[\d,]+', review_text.replace(',', ''))
            review_count = int(match.group()) if match else 0
        except Exception:
            review_count = 0
        # Validate with Pydantic
        result = HotelRating(name=f"{hotel_name}", rating=rating if rating else 0.0, review_count=review_count)
        print(f"Finished scrape for {hotel_name}: {result.rating}/5, {result.review_count} reviews")
        return result.dict(),time.time() - start_time
    except Exception as e:
        print(f"Error scraping {hotel_name}: {e}")
        # Save page source for debugging with unique filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        with open(f"debug_{hotel_name}_{timestamp}.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        return {"name": hotel_name, "rating": 0.0, "review_count": 0}
    finally:
        driver.quit()

# Save results to a text file
def save_results_to_file(results: list, time_taken: float, filename: str = "hotel_ratings.txt"):
    """
    Save scraped hotel results to a text file.
    Create file if it doesn't exist; append with incrementing 'Response X' label.
    """
    # Count existing responses
    response_number = 1
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            content = f.read()
            matches = re.findall(r"Response \d+", content)
            response_number = len(matches) + 1 if matches else 1
    
    # Format results
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    output = f"\nResponse {response_number} ({timestamp})\n"
    output += "=" * 50 + "\n"
    output += f"Total time taken: {time_taken:.2f} seconds\n"
    for result in sorted(results, key=lambda x: x['name']):
        output += f"{result['name']}: {result['rating']}/5, {result['review_count']} reviews\n"
    output += "=" * 50 + "\n"
    
    # Append to file
    with open(filename, "a", encoding="utf-8") as f:
        f.write(output)

def save_hotels_to_csv(hotel_data, filename="hotels_with_ratings.csv"):
    """
    Save a list of hotel data to a CSV file.

    Parameters:
    - hotel_data: List of dictionaries with 'Hotel Name', 'Rating', and 'Reviews'
    - filename: Name of the CSV file to save
    """
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=['name', 'rating', 'review_count'])
        writer.writeheader()
        writer.writerows(hotel_data)

def main():
    # Load hotel names from a CSV
    csv_path = "hotels.csv"  # Replace with your actual path or use argparse to make it dynamic
    try:
        df = pd.read_csv(csv_path)

        # Step 2: Ensure there's a column named 'hotel_name'
        if 'hotel_name' not in df.columns:
            raise ValueError("CSV must contain a 'hotel_name' column.")

        hotel_list = df['hotel_name'].dropna().unique().tolist()
        print(f"Loaded {len(hotel_list)} hotels from {csv_path}")
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return

    # Step 3: Proceed with the rest of your scraping logic
    results = []
    durations = {}
    t0 = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(scrape_hotel_rating, hotel): hotel for hotel in hotel_list}
        for future in concurrent.futures.as_completed(futures, timeout=5000):
            hotel = futures[future]
            try:
                result, duration = future.result()
                results.append(result)
                durations[hotel] = duration
                print(f"Thread completed for {hotel}")
            except concurrent.futures.TimeoutError:
                print(f"Timeout scraping {hotel}")
                results.append({"name": hotel, "rating": 0.0, "review_count": 0})
                durations[hotel] = None
            except Exception as e:
                print(f"Error scraping {hotel}: {e}")
                results.append({"name": hotel, "rating": 0.0, "review_count": 0})
                durations[hotel] = None

    t1 = time.time()
    save_results_to_file(results, t1 - t0)
    save_hotels_to_csv(results)

    print("\n=== Final Results ===")
    for result in sorted(results, key=lambda x: x['name']):
        print(f"{result['name']}: {result['rating']}/5, {result['review_count']} reviews")
    


    print(f"Total time taken: {t1 - t0:.2f} seconds")
    print("\n=== Scrape Durations (Per Hotel) ===")
    for hotel in sorted(durations):
        dur = durations[hotel]
        print(f"{hotel}: {'Failed' if dur is None else f'{dur:.2f} seconds'}")

if __name__ == "__main__":
    main()