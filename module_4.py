import json
import psycopg2
from datetime import datetime

# PostgreSQL Database Connection Settings
DB_CONFIG = {
    "dbname": "mlops_assignment",
    "user": "postgres",
    "password": "Sakthe@1010",  # Replace with your actual password
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

def save_to_postgresql(stories):
    """
    Saves extracted stories from JSON into the PostgreSQL database.
    """
    conn = None  # Initialize conn to None to prevent unbound errors

    try:
        # Establish database connection
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for story in stories:
            # Insert image data and return image_id
            cursor.execute(
                "INSERT INTO images (file_path) VALUES (%s) RETURNING id;", 
                (story["thumbnail_local_path"],)
            )
            image_id = cursor.fetchone()[0]  # Get the inserted image ID

            # Insert article metadata
            cursor.execute("""
                INSERT INTO articles (headline, url, scrape_time, article_date, image_id)
                VALUES (%s, %s, %s, %s, %s);
            """, (
                story["headline"],
                story.get("url", "No URL"),  # Handle missing URL cases
                datetime.now(),
                story.get("article_date", None),  # Handle missing article date
                image_id,
            ))

        # Commit changes to the database
        conn.commit()
        print(f"Inserted {len(stories)} stories into the database.")

    except Exception as e:
        print(f"Error saving to database: {e}")

    finally:
        # Ensure conn is closed only if it was successfully opened
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    # Load JSON data from Module 3 output
    json_file = "news_stories.json"  # Ensure this file exists
    stories = load_json_data(json_file)

    if stories:
        save_to_postgresql(stories)
    else:
        print("No stories found in JSON file.")
