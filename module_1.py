import json
import argparse
import requests

def load_config():
    """Loads configuration from a JSON file."""
    with open("config.json", "r") as file:
        return json.load(file)

def fetch_google_news_homepage(url):
    """Fetches the HTML content of Google News homepage."""
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.text
    else:
        print(f"Failed to fetch Google News, Status Code: {response.status_code}")
        return None

if __name__ == "__main__":
    # Argument parser for CLI override
    parser = argparse.ArgumentParser(description="Scrape the Google News homepage")
    parser.add_argument("--url", type=str, help="Google News homepage URL")
    args = parser.parse_args()

    # Load config and override with CLI arguments if provided
    config = load_config()
    news_homepage = args.url if args.url else config["news_homepage"]

    print(f"Fetching Google News homepage: {news_homepage}")
    html_content = fetch_google_news_homepage(news_homepage)

    if html_content:
        # Save the HTML content to a file
        with open("google_news_homepage.html", "w", encoding="utf-8") as file:
            file.write(html_content)
        print("\nGoogle News homepage successfully fetched and saved as 'google_news_homepage.html'.")
    else:
        print("\nFailed to fetch Google News homepage.")

