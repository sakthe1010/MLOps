import psycopg2
from difflib import SequenceMatcher

DB_CONFIG = {
    "dbname": "mlops_assignment",
    "user": "postgres",
    "password": "Sakthe@1010",
    "host": "localhost",
    "port": 5432
}

def similar_text(a, b):
    """Returns a similarity ratio between two text strings."""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def remove_duplicates():
    """Finds and removes duplicate articles based on headline similarity and exact URL match."""
    conn = None  

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Fetch all articles sorted by most recent first
        cursor.execute("SELECT id, headline, url, scrape_time FROM articles ORDER BY scrape_time DESC;")
        articles = cursor.fetchall()

        checked_articles = set()
        duplicates_removed = 0

        for i in range(len(articles)):
            article_id, headline, url, scrape_time = articles[i]

            for j in range(i + 1, len(articles)):
                compare_id, compare_headline, compare_url, compare_time = articles[j]

                # Remove duplicate if URL is the same
                if url == compare_url:
                    cursor.execute("DELETE FROM articles WHERE id = %s;", (compare_id,))
                    duplicates_removed += 1
                    continue

                # Remove duplicate if headline is 85% similar
                elif similar_text(headline, compare_headline) >= 0.85:
                    cursor.execute("DELETE FROM articles WHERE id = %s;", (compare_id,))
                    duplicates_removed += 1

        conn.commit()
        print(f"Removed {duplicates_removed} duplicate articles.")

    except Exception as e:
        error_message = str(e).encode('utf-8', 'ignore').decode('utf-8')
        print(f"Error removing duplicates: {error_message}")

    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    remove_duplicates()
