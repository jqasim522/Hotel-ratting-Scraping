import streamlit as st
import pandas as pd
import time
import concurrent.futures
from datetime import datetime
from scraping import scrape_hotel_rating, save_results_to_file  # Make sure to import your scrape_hotel_rating function

# Sample hotel list
hotel_list = [
        "New York Hilton Midtown", "London Marriott Hotel", "Sheraton Paris Charles de Gaulle Airport Hotel", "Ritz-Carlton Tokyo", "Four Seasons Sydney",
        "Waldorf Astoria Dubai Palm Jumeirah", "Mandarin Oriental, Hong Kong", "InterContinental Singapore", "Fairmont San Francisco", "Hyatt Regency Chicago",
        "Grand Hyatt Seoul", "Shangri-La Bangkok", "The Peninsula Manila", "Raffles Istanbul", "St. Regis Mexico City",
        "Biltmore Los Angeles", "Plaza Athénée Paris", "Savoy London", "Copacabana Palace Rio de Janeiro", "Burj Al Arab Dubai",
        "Taj Mahal Palace Mumbai", "Palace Hotel Tokyo", "Hotel del Coronado San Diego", "The Breakers Palm Beach", "The Greenbrier West Virginia",
        "The Broadmoor Colorado Springs", "The Phoenician Scottsdale", "The Cloister Sea Island", "The Lodge at Pebble Beach", "The Inn at Little Washington",
        "The Jefferson Washington DC", "The Ahwahnee Yosemite", "The Stanley Hotel Estes Park", "The Brown Palace Denver", "The Pfister Hotel Milwaukee",
        "The Drake Chicago", "The Palmer House Chicago", "The Peabody Memphis", "The Roosevelt New Orleans", "The Adolphus Dallas",
        "The St. Anthony San Antonio", "The Menger Hotel San Antonio", "The Arizona Biltmore Phoenix", "The Beverly Hills Hotel Los Angeles", "The Chateau Marmont Los Angeles",
        "The Mission Inn Riverside", "The Hotel del Coronado San Diego", "The Fairmont San Francisco", "The Palace Hotel San Francisco", "The St. Francis San Francisco"
    ]
st.title("Hotel Ratings Scraper")
st.write("This app scrapes hotel ratings and review counts using Google Maps.")

start = st.button("Start Scraping")

if start:
    st.info("Scraping started. Please wait...")
    start_time = time.time()
    results = []
    durations = {}

    with st.spinner("Scraping in progress..."):
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(scrape_hotel_rating, hotel): hotel for hotel in hotel_list}
            for future in concurrent.futures.as_completed(futures):
                hotel = futures[future]
                try:
                    result, duration = future.result()
                    results.append(result)
                    durations[hotel] = duration
                except Exception as e:
                    st.error(f"Error scraping {hotel}: {e}")
                    results.append({"name": hotel, "rating": 0.0, "review_count": 0})
                    durations[hotel] = None

    total_time = time.time() - start_time
    st.success(f"Scraping complete in {total_time:.2f} seconds")
    
    # Display final results
    df = pd.DataFrame(results).sort_values("name")
    st.dataframe(df)
    save_results_to_file(results,total_time)
    # Display durations
    st.subheader("Scrape Durations (Per Hotel)")
    for hotel in sorted(durations):
      dur = durations[hotel]
      print(f"{hotel}: {'Failed' if dur is None else f'{dur:.2f} seconds'}")
    st.dataframe(pd.DataFrame.from_dict(durations, orient="index", columns=["Duration"]))
