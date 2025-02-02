import psycopg2
from difflib import SequenceMatcher

# PostgreSQL Database Configuration
DB_CONFIG = {
    "dbname": "mlops_assignment",
    "user": "postgres",
    "password": "Sakthe@1010",  # Change this!
    "host": "localhost",
    "port": 5432
}

def similar_text(a, b):
    """Returns a similarity ratio between two text strings."""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def remove_duplicates():
    """Finds and removes duplicate articles based on headline similarity."""
    conn = None  

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # **Fetch all headlines and article IDs**
        cursor.execute("SELECT id, headline, scrape_time FROM articles ORDER BY scrape_time DESC;")
        articles = cursor.fetchall()

        checked_articles = set()
        duplicates_removed = 0

        for i in range(len(articles)):
            article_id, headline, scrape_time = articles[i]

            for j in range(i + 1, len(articles)):
                compare_id, compare_headline, compare_time = articles[j]

                # **Check if the headlines are similar (85% similarity threshold)**
                if similar_text(headline, compare_headline) >= 0.85:
                    # **Keep the most recent article and delete the older one**
                    older_id = compare_id  # The older article (lower in the list)
                    cursor.execute("DELETE FROM articles WHERE id = %s;", (older_id,))
                    duplicates_removed += 1

                    print(f"Removed duplicate article ID {older_id} (Headline: {compare_headline})")

            checked_articles.add(article_id)

        conn.commit()
        print(f"Removed {duplicates_removed} duplicate articles.")

    except Exception as e:
        print(f"Error removing duplicates: {e}")

    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    remove_duplicates()
