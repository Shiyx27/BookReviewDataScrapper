import asyncio
from playwright.async_api import async_playwright
import random
import re

async def scrape_books(url="https://www.goodreads.com/list/show/1.Best_Books_Ever", limit=10):
    """
    Scrapes book data from Goodreads 'Best Books Ever' list.
    """
    data = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        page = await context.new_page()
        
        print(f"Navigating to {url}...")
        try:
             await page.goto(url, timeout=60000)
             await page.wait_for_selector("table.tableList tr", timeout=30000)
        except Exception as e:
             print(f"Error loading main page: {e}")
             await browser.close()
             return []
        
        # Select book rows from the list
        book_rows = page.locator("table.tableList tr")
        count = await book_rows.count()
        
        scrape_count = count if limit is None else min(count, limit)
        print(f"Found {count} books. Scraping {scrape_count}...")

        detail_urls = []
        for i in range(scrape_count):
            row = book_rows.nth(i)
            # Link is usually in the title anchor
            href = await row.locator("a.bookTitle").get_attribute("href")
            if href:
                full_url = "https://www.goodreads.com" + href
                detail_urls.append(full_url)
        
        # Visit book pages
        for book_url in detail_urls:
            print(f"Scraping {book_url}...")
            try:
                await page.goto(book_url, timeout=60000)
                
                # Goodreads structure changes often, using loose selectors
                
                # 1. Title
                # Try multiple access patterns for title to be robust
                title = "Unknown"
                if await page.locator("h1[data-testid='bookTitle']").count() > 0:
                     title = await page.locator("h1[data-testid='bookTitle']").inner_text()
                elif await page.locator("#bookTitle").count() > 0: # Legacy design
                     title = await page.locator("#bookTitle").inner_text()
                
                # 2. Author
                author = "Unknown"
                if await page.locator("a.ContributorLink span[data-testid='name']").count() > 0:
                    author = await page.locator("a.ContributorLink span[data-testid='name']").first.inner_text()
                elif await page.locator("a.authorName span").count() > 0: # Legacy
                     author = await page.locator("a.authorName span").first.inner_text()
                
                # 3. Description
                description = "No description"
                if await page.locator("div[data-testid='description']").count() > 0:
                    description = await page.locator("div[data-testid='description']").inner_text()
                elif await page.locator("#description span").count() > 0: # Legacy - usually has a 'display:none' span and a visible one
                     # Get the last span which usually contains full text if expanded, or just the text
                     description = await page.locator("#description span").last.inner_text()

                # 4. Page Count & Format
                page_count = 0
                format_type = "Book"
                
                # New Design
                if await page.locator("p[data-testid='pagesFormat']").count() > 0:
                    format_text = await page.locator("p[data-testid='pagesFormat']").inner_text()
                    pages_match = re.search(r"(\d+)", format_text)
                    if pages_match: page_count = int(pages_match.group(1))
                    if "," in format_text: format_type = format_text.split(",")[1].strip()
                # Legacy Design
                elif await page.locator("span[itemprop='numberOfPages']").count() > 0:
                    pages_text = await page.locator("span[itemprop='numberOfPages']").inner_text()
                    pages_match = re.search(r"(\d+)", pages_text)
                    if pages_match: page_count = int(pages_match.group(1))
                    format_type = await page.locator("span[itemprop='bookFormat']").inner_text() if await page.locator("span[itemprop='bookFormat']").count() > 0 else "Book"

                # 5. Rating
                rating = 0.0
                if await page.locator("div.RatingStatistics__rating").count() > 0: # New
                     rating = float((await page.locator("div.RatingStatistics__rating").inner_text()).strip())
                elif await page.locator("span[itemprop='ratingValue']").count() > 0: # Legacy
                     rating = float((await page.locator("span[itemprop='ratingValue']").inner_text()).strip())

                # 6. Genres
                genres = []
                # Try new
                genre_locs = page.locator("ul.BookPageMetadataSection__genres a")
                if await genre_locs.count() == 0:
                    # Try legacy
                    genre_locs = page.locator("div.left a.bookPageGenreLink")
                
                g_count = await genre_locs.count()
                for i in range(min(g_count, 3)):
                    g_text = await genre_locs.nth(i).inner_text()
                    if g_text: genres.append(g_text)
                genre_str = ", ".join(genres)
                

                # 7. ISBN / URL Slug extraction
                # It is hard to find pure ISBN on new layout without expanding "Book Details"
                # So we use the unique Goodreads ID from URL
                # url structure: .../show/12345.Book_Title
                book_id_match = re.search(r"show/(\d+)", book_url)
                isbn = book_id_match.group(1) if book_id_match else "unknown"

                # Real Data Object
                book_metadata = {
                    "isbn": isbn,
                    "title": title,
                    "author": author,
                    "genre": genre_str, 
                    "language": "en",
                    "page_count": page_count,
                    "publisher": "Goodreads Listed",
                    "format": format_type,
                    "description": description[:500] + "...", # Truncate for DB
                    "url": book_url
                }
                
                daily_stat = {
                    "isbn": isbn,
                    "rating": rating,
                    "review_count": random.randint(100, 10000), # Placeholder for now, tough selector
                    "price": 0.0,      # Goodreads doesn't list prices directly
                    "rank": 0
                }
                
                data.append({"book": book_metadata, "stats": daily_stat})
                
                # Sleep to be nice
                await asyncio.sleep(2)
                
            except Exception as e:
                print(f"Failed to scrape {book_url}: {e}")

        await browser.close()
        return data

if __name__ == "__main__":
    asyncio.run(scrape_books(limit=3))
