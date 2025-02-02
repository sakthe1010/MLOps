import json
import psycopg2
from datetime import datetime
from psycopg2.extras import execute_values
from difflib import SequenceMatcher

# PostgreSQL Database Configuration
DB_CONFIG = {
    "dbname": "mlops_assignment",
    "user": "postgres",
    "password": "Sakthe@1010",  # Change this!
    "host": "localhost",
    "port": 5432
}

def load_json_data(json_file):
    """
    Load scraped news stories from a JSON file.
    """
    try:
        with open(json_file, "r", encoding="utf-8") as file:
            stories = json.load(file)
        return stories
    except Exception as e:
        print(f"Error loading JSON file: {e}")
        return []

def similar_text(a, b):
    """
    Returns a similarity ratio between two text strings.
    """
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def check_duplicate(cursor, headline, url, article_date):
    """
    Checks if a similar article already exists in the database.
    - **URL match**: Ensures the same article URL is not inserted twice.
    - **Headline similarity**: Checks if an article with a similar headline is already present.
    - **Article date match**: Ensures no duplicate news for the same day.
    """
    cursor.execute("""
        SELECT id, headline, url, article_date FROM articles
        WHERE article_date = %s;
    """, (article_date,))

    existing_articles = cursor.fetchall()
    
    for article in existing_articles:
        existing_id, existing_headline, existing_url, existing_date = article

        # **Check if URL is the same (Exact match)**
        if url == existing_url:
            print(f"Duplicate URL found. Skipping insert.")
            return True  # URL is already present, skip this article
        
        # **Check Headline Similarity (85% threshold)**
        if similar_text(headline, existing_headline) >= 0.85:
            print(f"Duplicate headline Found. Skipping insert.")
            return True  # Similar headline exists, skip this article

    return False  # No duplicate found, proceed with insertion

def save_to_postgresql(stories):
    """
    Saves extracted stories from JSON into PostgreSQL, avoiding duplicates.
    """
    conn = None  
    inserted_count = 0  # ✅ Track successfully inserted articles

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for story in stories:
            headline = story["headline"]
            url = story["url"]
            article_date = story["article_date"]
            scrape_time = story["scrape_timestamp"]
            image_path = story["thumbnail_local_path"]

            # **Check for duplicates before inserting**
            if check_duplicate(cursor, headline, url, article_date):
                continue  # Skip insertion if duplicate is found

            # **Insert Image**
            cursor.execute(
                "INSERT INTO images (file_path) VALUES (%s) RETURNING id;",
                (image_path,)
            )
            image_id = cursor.fetchone()[0]

            # **Insert Article**
            cursor.execute("""
                INSERT INTO articles (headline, url, scrape_time, article_date, image_id)
                VALUES (%s, %s, %s, %s, %s);
            """, (headline, url, scrape_time, article_date, image_id))

            inserted_count += 1  # ✅ Increment only when a new article is inserted

        # Commit all changes
        conn.commit()

        # ✅ Correct Output Message
        if inserted_count > 0:
            print(f"Successfully inserted {inserted_count} new stories into the database.")
        else:
            print("No new stories added. All were duplicates.")

    except Exception as e:
        print(f"⚠️ Error saving to database: {e}")

    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    json_file = "news_stories.json"  # Output from Module 3
    stories = load_json_data(json_file)

    if stories:
        save_to_postgresql(stories)
    else:
        print("⚠️ No stories found in JSON file.")
