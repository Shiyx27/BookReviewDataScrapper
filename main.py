import asyncio
import database
import scraper
from datetime import datetime

async def main():
    print("Starting Book Data Collection Pipeline...")
    
    # 1. Initialize Database
    print("Initializing Database...")
    database.init_db()
    
    # 2. Run Scraper
    print("Running Scraper...")
    try:
        # Scrape top 50 books from the list
        scraped_data = await scraper.scrape_books(limit=50) 
        print(f"Successfully scraped {len(scraped_data)} items.")
        
        # 3. Insert Data into Database
        print("Inserting data into database...")
        for item in scraped_data:
            book_info = item['book']
            stats_info = item['stats']
            
            # Upsert Book Metadata (Static)
            database.upsert_book(book_info)
            
            # Insert Daily Stats (Dynamic)
            # Add timestamp here or let database.py handle it (it handles it if missing)
            stats_info['scrape_date'] = datetime.now()
            database.insert_daily_stats(stats_info)
            
        print("Data processing complete.")
        
    except Exception as e:
        print(f"An error occurred during the pipeline execution: {e}")

if __name__ == "__main__":
    asyncio.run(main())
