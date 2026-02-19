import sqlite3
from datetime import datetime
import os

DB_NAME = "book_tracker.db"

def get_connection():
    """Establishes a connection to the SQLite database."""
    return sqlite3.connect(DB_NAME)

def init_db():
    """Initializes the database with the required tables."""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Enable foreign key support
    cursor.execute("PRAGMA foreign_keys = ON;")

    # Table 1: books (Static Metadata)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS books (
            isbn TEXT PRIMARY KEY,
            title TEXT,
            author TEXT,
            genre TEXT,
            language TEXT,
            page_count INTEGER,
            publisher TEXT,
            format TEXT,
            description TEXT,
            url TEXT
        )
    """)

    # Table 2: daily_stats (Dynamic Time-Series)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            isbn TEXT,
            scrape_date DATE,
            rating REAL,
            review_count INTEGER,
            price REAL,
            rank INTEGER,
            FOREIGN KEY (isbn) REFERENCES books (isbn)
        )
    """)

    # Index on scrape_date
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_scrape_date ON daily_stats (scrape_date)
    """)

    conn.commit()
    conn.close()
    print(f"Database {DB_NAME} initialized.")

def upsert_book(book_data):
    """
    Inserts a book if the ISBN doesn't exist.
    book_data should be a dictionary matching the books table columns.
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT OR IGNORE INTO books (
                isbn, title, author, genre, language, page_count, 
                publisher, format, description, url
            ) VALUES (
                :isbn, :title, :author, :genre, :language, :page_count, 
                :publisher, :format, :description, :url
            )
        """, book_data)
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error upserting book {book_data.get('isbn')}: {e}")
    finally:
        conn.close()

def insert_daily_stats(stats_data):
    """
    Inserts a new record into daily_stats.
    stats_data should be a dictionary matching the daily_stats table columns.
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # Ensure scrape_date is set
    if 'scrape_date' not in stats_data:
        stats_data['scrape_date'] = datetime.now()

    try:
        cursor.execute("""
            INSERT INTO daily_stats (
                isbn, scrape_date, rating, review_count, price, rank
            ) VALUES (
                :isbn, :scrape_date, :rating, :review_count, :price, :rank
            )
        """, stats_data)
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error inserting stats for {stats_data.get('isbn')}: {e}")
    finally:
        conn.close()
