import json
import psycopg2
from datetime import datetime
from psycopg2.extras import execute_values
from difflib import SequenceMatcher

# PostgreSQL Database Configuration
DB_CONFIG = {
    "dbname": "mlops_assignment",
    "user": "postgres",
    "password": "Sakthe@1010", 
    "host": "localhost",
    "port": 5432
}

def load_json_data(json_file):
    """Load scraped news stories from a JSON file."""
    try:
        with open(json_file, "r", encoding="utf-8") as file:
            stories = json.load(file)
        return stories
    except Exception as e:
        print(f"Error loading JSON file: {e}")
        return []

def read_image_as_binary(image_path):
    """Reads an image file and converts it into binary data."""
    try:
        with open(image_path, "rb") as img_file:
            return img_file.read()
    except Exception as e:
        print(f"Error reading image {image_path}: {e}")
        return None

def similar_text(a, b):
    """Returns a similarity ratio between two text strings."""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def check_duplicate(cursor, headline, url, article_date):
    """Check if an article is already stored in the database."""
    cursor.execute("""
        SELECT id, headline, url, article_date FROM articles
        WHERE article_date = %s;
    """, (article_date,))

    existing_articles = cursor.fetchall()
    
    for article in existing_articles:
        existing_id, existing_headline, existing_url, existing_date = article
        
        # **Check Headline Similarity (85% threshold)**
        if similar_text(headline, existing_headline) >= 0.85:
            print(f"Duplicate headline found. Skipping insert.")
            return True  

    return False  # No duplicate found

def save_to_postgresql(stories):
    """Saves extracted stories from JSON into PostgreSQL, avoiding duplicates."""
    conn = None  
    inserted_count = 0  

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for story in stories:
            headline = story["headline"]
            url = story["url"]
            article_date = story["article_date"]
            scrape_time = f"{article_date} {story['scrape_timestamp']}"  # ✅ Convert to Full Timestamp
            image_path = story["thumbnail_local_path"]

            # **Check for duplicates before inserting**
            if check_duplicate(cursor, headline, url, article_date):
                continue  # Skip duplicate

            # **Read Image as Binary**
            image_binary = read_image_as_binary(image_path)
            if not image_binary:
                print(f"Skipping article due to missing image: {image_path}")
                continue

            # **Insert Image into Database**
            cursor.execute(
                "INSERT INTO images (image_data) VALUES (%s) RETURNING id;",
                (psycopg2.Binary(image_binary),)
            )
            image_id = cursor.fetchone()[0]

            # **Insert Article**
            cursor.execute("""
                INSERT INTO articles (headline, url, scrape_time, article_date, image_id)
                VALUES (%s, %s, %s, %s, %s);
            """, (headline, url, scrape_time, article_date, image_id))

            inserted_count += 1  # ✅ Track inserted records

        conn.commit()

        # ✅ Correct Output Message
        if inserted_count > 0:
            print(f"Successfully inserted {inserted_count} new stories into the database.")
        else:
            print("No new stories added. All were duplicates.")

    except Exception as e:
        print(f"Error saving to database: {str(e).encode('utf-8', 'ignore').decode('utf-8')}")  # ✅ Prevent Unicode Errors

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
        print("No stories found in JSON file.")
