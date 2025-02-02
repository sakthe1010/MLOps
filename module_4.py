import json
import psycopg2
import os
from datetime import datetime

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
        if not os.path.exists(image_path):  # Check if file exists
            print(f"Image file not found: {image_path}")
            return None
        with open(image_path, "rb") as img_file:
            return img_file.read()
    except Exception as e:
        print(f"Error reading image {image_path}: {e}")
        return None

def save_to_postgresql(stories):
    """Inserts all scraped stories into PostgreSQL (duplicates included)."""
    conn = None  

    try:
        print("Connecting to PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("Connection established.")

        for story in stories:
            try:
                headline = story["headline"]
                url = story["url"]
                article_date = story["article_date"]
                scrape_time = f"{article_date} {story['scrape_timestamp']}"  # Convert to Full Timestamp
                image_path = story["thumbnail_local_path"]

                # Read Image as Binary
                image_binary = read_image_as_binary(image_path)
                if not image_binary:
                    print(f"Skipping article due to missing image: {image_path}")
                    continue

                # Insert Image into Database
                cursor.execute(
                    "INSERT INTO images (image_data) VALUES (%s) RETURNING id;",
                    (psycopg2.Binary(image_binary),)
                )
                image_id = cursor.fetchone()[0]

                # Insert Article 
                cursor.execute("""
                    INSERT INTO articles (headline, url, scrape_time, article_date, image_id)
                    VALUES (%s, %s, %s, %s, %s);
                """, (headline, url, scrape_time, article_date, image_id))

            except Exception as e:
                print(f"Error inserting article: {story.get('headline', 'Unknown')} - {e}")

        conn.commit()
        print("Successfully inserted all stories (duplicates included) into the database.")

    except Exception as e:
        print(f"Error connecting to database: {e}")

    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Connection closed.")

if __name__ == "__main__":
    json_file = "news_stories.json"
    stories = load_json_data(json_file)

    if stories:
        print(f"Loaded {len(stories)} stories from JSON.")
        save_to_postgresql(stories)
    else:
        print("No stories found in JSON file.")
