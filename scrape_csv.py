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
from urllib.parse import quote
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Define a structure to ensure hotel ratings are valid
class HotelRating(BaseModel):
    id: str
    name: str
    address: str
    rating: float = Field(..., ge=0, le=5, description="Hotel star rating (0-5)")
    review_count: int = Field(..., ge=0, description="Number of reviews")

# Create a Google Maps search link for the hotel
def form_search_url(hotel_name: str, address: str) -> str:
    # Check if "jeddah" is already present (case-insensitive)
    keywords = ["hotel", "resort", "inn", "lodge", "suites", "guest house", "residence", "hostel", "palace", "apartments"]
    if not any(keyword in hotel_name.lower() for keyword in keywords):
        hotel_name += " hotel"
    if not any(keyword in hotel_name.lower() for keyword in ["pakistan"]):
        hotel_name += " pakistan"
    encoded_query = quote(hotel_name)
    return f"https://www.google.com/maps/search/{encoded_query}"

# Scrape Google Maps for hotel rating and review count
def scrape_hotel_rating(hotel_id: str, hotel_name: str, address: str) -> dict:
    start_time = time.time()
    print(f"Starting scrape for {hotel_name} at {datetime.now().strftime('%H:%M:%S')}")
    url = form_search_url(hotel_name, address)
    
    # Create a new driver instance for each hotel
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-webgl")
    driver = None
    
    try:
        driver = webdriver.Chrome(options=options)
        driver.get(url)
        # Wait for results to load
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="article"]'))
            )
        except Exception as e:
            print(f"Element not found for {hotel_name}: {e}")

        rating = None
        review_count = None
        # Try clicking up to 3 results if available
        for idx in range(3):
            try:
                results = driver.find_elements(By.CSS_SELECTOR, 'div[role="article"]')
                if results and idx < len(results):
                    results[idx].click()
                    time.sleep(3)  # Increased to avoid CAPTCHAs
            except Exception:
                continue
            # Try all selectors for rating
            selectors = [
                'span[aria-label*="stars"]',
                'span.MW4etd',
                '[aria-label*="Rated"]',
                '[aria-label*="out of 5"]',
                'meta[itemprop="ratingValue"]',
                'span[jsname="Te9Tpc"]',
                '.aMPvhf-fI6EEc-KVuj8d'
            ]
            for sel in selectors:
                try:
                    elems = driver.find_elements(By.CSS_SELECTOR, sel)
                    for elem in elems:
                        aria = elem.get_attribute('aria-label') or elem.text
                        match = re.search(r'(\d+\.\d+|\d+)', aria)
                        if match:
                            rating = float(match.group(1))
                            print("ARIA text found:", aria)
                            # Try to extract review count from the same ARIA text if present
                            review_match = re.search(r'([\d,]+)\s+Reviews?', aria, re.IGNORECASE)
                            if review_match:
                                review_count = int(review_match.group(1).replace(',', ''))
                                break  # Found review count, break out of selector loop
                            break
                    if rating:
                        break
                except Exception:
                    continue
            # Only run review selector loop if review_count not found
            if review_count is None:
                review_selectors = [
                    'span.OEwtMc',
                    'button[jsaction*="pane.rating.moreReviews"]',
                    'span[aria-label*="reviews"]',
                    'span[class*="review"]',
                    'div[class*="review"]'
                ]
                for sel in review_selectors:
                    try:
                        elems = driver.find_elements(By.CSS_SELECTOR, sel)
                        for elem in elems:
                            text = elem.get_attribute('aria-label') or elem.text
                            match = re.search(r'[\d,]+', text.replace(',', ''))
                            if match:
                                review_count = int(match.group())
                                break
                        if review_count is not None:
                            break
                    except Exception:
                        continue
            if rating is not None or review_count is not None:
                break
        # Fallback: try meta tag or visible text if still not found
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
        if review_count is None:
            review_count = 0
        result = HotelRating(
            id=str(hotel_id),
            name=hotel_name,
            address=address,
            rating=rating if rating else 0.0,
            review_count=review_count
        )
        print(f"Finished scrape for {hotel_name}: {result.rating}/5, {result.review_count} reviews")
        return result.dict(), time.time() - start_time

    except Exception as e:
        print(f"Error scraping {hotel_name}: {e}")
        if driver:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            with open(f"debug_{hotel_name}_{timestamp}.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
        return {"id": str(hotel_id), "name": hotel_name, "address": address, "rating": 0.0, "review_count": 0}
    
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

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

def update_csv_with_ratings(original_df, merged_df):
    """
    Update original_df with rating and review_count from merged_df by matching 'id'.
    Adds 'rating' and 'review_count' columns if not present.
    Does not modify any other columns.
    """
    # Only keep id, rating, review_count from merged_df
    ratings = merged_df[['id', 'rating', 'review_count']]
    # Merge on 'id', updating only rating/review_count
    updated_df = original_df.copy()
    updated_df = updated_df.merge(ratings, on='id', how='left', suffixes=('', '_new'))
    updated_df['rating'] = updated_df['rating_new']
    updated_df['review_count'] = updated_df['review_count_new']
    updated_df.drop(['rating_new', 'review_count_new'], axis=1, inplace=True)
    return updated_df

def main():
    csv_path = "Pakistan Hotels List.csv"
    output_path = "pakistan_hotels_updated.csv"
    
    try:
        df = pd.read_csv(csv_path)
        scraped_ids = set()
        if os.path.exists(output_path):
            scraped_df = pd.read_csv(output_path)
            scraped_ids = set(scraped_df['id'].astype(str))
        
        required_cols = {'id', 'name', 'address'}
        if not required_cols.issubset(df.columns):
            raise ValueError(f"CSV must contain columns: {required_cols}")
        
        # Only hotels not already scraped
        hotel_list = df[~df['id'].astype(str).isin(scraped_ids)][['id', 'name', 'address']].dropna().values.tolist()
        print(f"Loaded {len(hotel_list)} hotels to scrape")
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return

    # Scrape ratings and reviews
    results = []
    durations = {}
    t0 = time.time()
    
    # Each thread gets its own WebDriver instance
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(scrape_hotel_rating, hotel_id, hotel_name, address): (hotel_id, hotel_name) 
                  for hotel_id, hotel_name, address in hotel_list}
        
        for future in concurrent.futures.as_completed(futures, timeout=10000):
            hotel_id, hotel_name = futures[future]
            try:
                result, duration = future.result()
                if result['name'] == hotel_name:  # Verify result matches the hotel
                    results.append(result)
                    durations[hotel_id] = duration
                    print(f"Thread completed for {hotel_name}")
                    print(f"Duration for {hotel_id}: {duration:.2f} seconds")
                    
                    # Save rating/review to CSV after each thread
                    with open(output_path, mode='a', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=['id', 'name', 'address', 'rating', 'review_count'])
                        if f.tell() == 0:
                            writer.writeheader()
                        writer.writerow(result)
                else:
                    print(f"Warning: Result mismatch for {hotel_name}")
                    
            except concurrent.futures.TimeoutError:
                print(f"Timeout scraping {hotel_name}")
                durations[hotel_id] = None
            except Exception as e:
                print(f"Error scraping {hotel_name}: {e}")
                durations[hotel_id] = None

    t1 = time.time()

    if results:
        # results_df = pd.DataFrame(results)
        # results_df = results_df.sort_values(by='id', ascending=True)
        # results_df.to_csv(output_path, index=False)

        print("\n=== Final Results ===")
        for result in sorted(results, key=lambda x: x['name']):
            print(f"{result['name']}: {result['rating']}/5, {result['review_count']} reviews")
    
    print(f"Total time taken: {t1 - t0:.2f} seconds")
    print("\n=== Scrape Durations (Per Hotel) ===")
    for hotel_id in sorted(durations):
        dur = durations[hotel_id]
        print(f"{hotel_id}: {'Failed' if dur is None else f'{dur:.2f} seconds'}")

if __name__ == "__main__":
    main()