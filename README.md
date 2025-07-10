Hotel Review & Rating Scraper
A robust Python-based tool to scrape real-time hotel ratings and review counts from Google Maps using Selenium and multithreading. It supports parallel execution and logs scrape duration and data validity for 50+ global hotels.

🚀 Features
🌐 Scrapes live hotel ratings and number of reviews from Google Maps

⚙️ Multithreaded execution for fast parallel scraping

✅ Validates scraped data using Pydantic (ensures proper structure and range)

🕵️ Captures scrape duration per hotel and total run time

🧾 Appends results into a cumulative report file with timestamp

🧪 Handles fallback selectors and page structure changes

🛠️ Easy to extend, modular scraping pipeline

📁 Project Structure

hotel_scraper/
├── scraper.py           # Main scraping logic
├── app.py               # Streamlit UI app
├── requirements.txt     # Dependencies
├── hotel_ratings.txt    # Generated results log
└── README.md
