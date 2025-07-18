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
from selenium.webdriver.common.action_chains import ActionChains
import threading

# Define a structure to ensure hotel ratings are valid
class HotelRating(BaseModel):
    id: str
    name: str
    address: str
    rating: float = Field(..., ge=0, le=5, description="Hotel star rating (0-5)")
    review_count: int = Field(..., ge=0, description="Number of reviews")

def create_driver():
    """Create a fresh WebDriver instance with optimized settings"""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-webgl")
    options.add_argument("--disable-images")  # Disable images for faster loading
    options.add_argument("--disable-plugins")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-features=TranslateUI")
    options.add_argument("--disable-ipc-flooding-protection")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Add unique user data directory for each instance to avoid conflicts
    import tempfile
    temp_dir = tempfile.mkdtemp()
    options.add_argument(f"--user-data-dir={temp_dir}")
    
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    # Performance optimizations
    prefs = {
        "profile.default_content_setting_values": {
            "images": 2,
            "plugins": 2,
            "popups": 2,
            "geolocation": 2,
            "notifications": 2,
            "media_stream": 2,
        }
    }
    options.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(options=options)
    # Set implicit wait for faster element finding
    driver.implicitly_wait(2)
    
    return driver

def extract_city_from_address(address: str) -> str:
    """Extract city from address - city is at third-last position"""
    if not address or pd.isna(address):
        return ""
    
    # Clean the address
    address = str(address).strip()
    
    # Split by comma and clean each part
    parts = [part.strip() for part in address.split(',') if part.strip()]
    
    # Check if we have enough parts to get the third-last one
    if len(parts) >= 3:
        # Get the third-last part (index -3)
        city = parts[-3].strip()
        
        # Clean up the city name - remove any extra spaces or formatting
        city = ' '.join(city.split())
        
        return city
    
    # If not enough parts, return empty string
    return ""

def form_search_url(hotel_name: str, address: str) -> str:
    """Create a Google Maps search link for the hotel with city included"""
    # Extract city from address
    city = extract_city_from_address(address)
    
    # Build search query
    search_query = hotel_name.strip()
    
    # Add hotel keyword if not present
    hotel_name_lower = hotel_name.lower()
    keywords = ["hotel", "resort", "inn", "lodge", "suites", "guest house", "residence", "hostel", "palace", "apartments"]
    
    if not any(keyword in hotel_name_lower for keyword in keywords):
        search_query += " hotel"
    
    # Add city if extracted
    if city:
        search_query += f" {city}"
    
    # Add UK if not already present
    if "uk" not in search_query.lower():
        search_query += " UK"
    
    # Encode and create URL
    encoded_query = quote(search_query)
    print(f"Search URL for {hotel_name}:", encoded_query)
    return f"https://www.google.com/maps/search/{encoded_query}"


# Optimized rating extraction function
def extract_rating_info(driver, hotel_name: str) -> tuple:
    """Extract rating and review count more efficiently"""
    rating = None
    review_count = None
    
    # Optimized selectors in priority order
    rating_selectors = [
        'span[aria-label*="stars"]',
        'span.MW4etd',
        '[aria-label*="Rated"]',
        '[aria-label*="out of 5"]',
        'meta[itemprop="ratingValue"]',
        'span[jsname="Te9Tpc"]',
        '.aMPvhf-fI6EEc-KVuj8d'
    ]
    
    review_selectors = [
        'span.OEwtMc',
        'button[jsaction*="pane.rating.moreReviews"]',
        'span[aria-label*="reviews"]',
        'span[class*="review"]',
        'div[class*="review"]'
    ]
    
    # Try to find rating first
    for selector in rating_selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for elem in elements:
                text = elem.get_attribute('aria-label') or elem.text or elem.get_attribute('data-value')
                if text:
                    # More efficient regex for rating
                    match = re.search(r'(\d+(?:\.\d+)?)', text)
                    if match:
                        potential_rating = float(match.group(1))
                        print("ARIA text found:", text)
                        print()
                        if 0 <= potential_rating <= 5:
                            rating = potential_rating
                            
                            # Try to extract review count from same element
                            review_match = re.search(r'([\d,]+)\s*(?:reviews?|Reviews?)', text, re.IGNORECASE)
                            if review_match:
                                review_count = int(review_match.group(1).replace(',', ''))
                            break
            if rating is not None:
                break
        except Exception:
            continue
    
    # If review count not found, search separately
    if review_count is None:
        for selector in review_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for elem in elements:
                    text = elem.get_attribute('aria-label') or elem.text
                    if text:
                        # Look for review count
                        match = re.search(r'[\d,]+', text.replace(',', ''))
                        if match:
                            review_count = int(match.group())
                            break
                if review_count is not None:
                    break
            except Exception:
                continue
    
    return rating or 0.0, review_count or 0

# Optimized scraping function with individual driver instances
def scrape_hotel_rating(hotel_id: str, hotel_name: str, address: str) -> dict:
    start_time = time.time()
    print(f"Starting scrape for {hotel_name} (ID: {hotel_id}) at {datetime.now().strftime('%H:%M:%S')}")
    
    url = form_search_url(hotel_name, address)
    driver = None
    
    try:
        # Create fresh driver instance for this specific hotel
        driver = create_driver()
        driver.get(url)
        
        # Reduced wait time with more specific condition
        try:
            WebDriverWait(driver, 8).until(
                EC.any_of(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="article"]')),
                    EC.presence_of_element_located((By.CSS_SELECTOR, '.W4Efsd')),
                    EC.presence_of_element_located((By.CSS_SELECTOR, '.hfpxzc'))
                )
            )
        except Exception as e:
            print(f"Timeout waiting for results for {hotel_name} (ID: {hotel_id}): {e}")
            

        rating = None
        review_count = None
        
        # Try to extract from current page first (often works without clicking)
        rating, review_count = extract_rating_info(driver, hotel_name)
        
        # If not found, try clicking on results (reduced from 3 to 2 attempts)
        if rating == 0.0 and review_count == 0:
            try:
                results = driver.find_elements(By.CSS_SELECTOR, 'div[role="article"], .hfpxzc')
                
                for idx in range(min(2, len(results))):
                    try:
                        # Use ActionChains for more reliable clicking
                        ActionChains(driver).move_to_element(results[idx]).click().perform()
                        time.sleep(2)  # Reduced wait time
                        
                        # Extract rating info
                        rating, review_count = extract_rating_info(driver, hotel_name)
                        
                        if rating > 0 or review_count > 0:
                            break
                            
                    except Exception as e:
                        print(f"Error clicking result {idx} for {hotel_name} (ID: {hotel_id}): {e}")
                        continue
                        
            except Exception as e:
                print(f"Error finding results for {hotel_name} (ID: {hotel_id}): {e}")
        
        # Create result with explicit verification
        result = HotelRating(
            id=str(hotel_id),
            name=hotel_name,
            address=address,
            rating=rating,
            review_count=review_count
        )
        
        print(f"✓ Finished scrape for {hotel_name} (ID: {hotel_id}): {result.rating}/5, {result.review_count} reviews")
        return result.dict(), time.time() - start_time

    except Exception as e:
        print(f"✗ Error scraping {hotel_name} (ID: {hotel_id}): {e}")
        return {"id": str(hotel_id), "name": hotel_name, "address": address, "rating": 0.0, "review_count": 0}, time.time() - start_time
    
    finally:
        # Always cleanup driver after each hotel
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

# Removed batch processing to avoid shared driver issues
def process_hotels_concurrently(hotel_list, max_workers=3):
    """Process hotels with individual driver instances per task"""
    results = []
    durations = {}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit each hotel as individual task with its own driver
        future_to_hotel = {
            executor.submit(scrape_hotel_rating, hotel_id, hotel_name, address): (hotel_id, hotel_name, address)
            for hotel_id, hotel_name, address in hotel_list
        }
        
        for future in concurrent.futures.as_completed(future_to_hotel, timeout=20000):
            hotel_id, hotel_name, address = future_to_hotel[future]
            try:
                result, duration = future.result()
                
                # Verify result matches the expected hotel (safety check)
                if result['id'] == str(hotel_id) and result['name'] == hotel_name:
                    results.append(result)
                    durations[hotel_id] = duration
                    print(f"Scrape Time for {hotel_name} is: {duration:.2f} seconds")
                    print(f"✓ Thread completed for {hotel_name} (ID: {hotel_id})")
                    
                    # Save result immediately
                    save_single_result(result)
                else:
                    print(f"⚠️ Result mismatch for {hotel_name} (ID: {hotel_id})")
                    # Save with correct data
                    correct_result = {
                        "id": str(hotel_id),
                        "name": hotel_name,
                        "address": address,
                        "rating": 0.0,
                        "review_count": 0
                    }
                    save_single_result(correct_result)
                    
            except concurrent.futures.TimeoutError:
                print(f"⏰ Timeout scraping {hotel_name} (ID: {hotel_id})")
                durations[hotel_id] = None
                # Save empty result for timeout
                timeout_result = {
                    "id": str(hotel_id),
                    "name": hotel_name,
                    "address": address,
                    "rating": 0.0,
                    "review_count": 0
                }
                save_single_result(timeout_result)
                
            except Exception as e:
                print(f"✗ Error scraping {hotel_name} (ID: {hotel_id}): {e}")
                durations[hotel_id] = None
                # Save empty result for error
                error_result = {
                    "id": str(hotel_id),
                    "name": hotel_name,
                    "address": address,
                    "rating": 0.0,
                    "review_count": 0
                }
                save_single_result(error_result)
    
    return results, durations

def save_single_result(result, output_path="updated\\UK_hotels_updated.csv"):
    """Save a single result to CSV file"""
    try:
        file_exists = os.path.exists(output_path)
        with open(output_path, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['id', 'name', 'address', 'rating', 'review_count'])
            if not file_exists:
                writer.writeheader()
            writer.writerow(result)
            print(f"✓ Result saved for {result['name']} (ID: {result['id']})")
    except Exception as e:
        print(f"Error saving result: {e}")

def save_results_to_file(results: list, time_taken: float, filename: str = "hotel_ratings.txt"):
    """Save scraped hotel results to a text file."""
    response_number = 1
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            content = f.read()
            matches = re.findall(r"Response \d+", content)
            response_number = len(matches) + 1 if matches else 1
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    output = f"\nResponse {response_number} ({timestamp})\n"
    output += "=" * 50 + "\n"
    output += f"Total time taken: {time_taken:.2f} seconds\n"
    for result in sorted(results, key=lambda x: x['name']):
        output += f"{result['name']}: {result['rating']}/5, {result['review_count']} reviews\n"
    output += "=" * 50 + "\n"
    
    with open(filename, "a", encoding="utf-8") as f:
        f.write(output)

def main():
    csv_path = "List\\UK Hotels.csv"
    output_path = "updated\\UK_hotels_updated.csv"

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
        
        if not hotel_list:
            print("No hotels to scrape!")
            return
            
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return

    # Process hotels with individual driver instances
    results = []
    durations = {}
    t0 = time.time()
    
    print("Starting concurrent processing with individual driver instances...")
    results, durations = process_hotels_concurrently(hotel_list, max_workers=5)
    
    t1 = time.time()

    # Final processing
    if results:
        try:
            df = pd.read_csv(output_path)
            df = df.sort_values(by='id', ascending=True)
            df.to_csv(output_path, index=False)
            print("✓ Results saved and sorted successfully")
        except Exception as e:
            print(f"Error sorting results: {e}")

        print("\n" + "="*60)
        print("FINAL RESULTS SUMMARY")
        print("="*60)
        for result in sorted(results, key=lambda x: x['name']):
            print(f"{result['name']} (ID: {result['id']}): {result['rating']}/5, {result['review_count']} reviews")
    
    print(f"\n⏱️ Total time taken: {t1 - t0:.2f} seconds")
    print(f"📊 Average time per hotel: {(t1 - t0) / len(hotel_list):.2f} seconds")
    print(f"✅ Successfully processed: {len(results)} hotels")
    print(f"⚠️ Failed/Timeout: {len(hotel_list) - len(results)} hotels")
    
    # Save summary
    save_results_to_file(results, t1 - t0)

if __name__ == "__main__":
    main()