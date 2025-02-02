import argparse
import json
from bs4 import BeautifulSoup

def read_html_file(file_path):
    """Reads the saved Google News homepage HTML file."""
    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()

def extract_top_stories_link(html_content):
    """Dynamically extracts the 'Top Stories' section link from the Google News homepage without relying on CSS classes."""
    soup = BeautifulSoup(html_content, "html.parser")

    # Find all <a> tags
    for link in soup.find_all("a", href=True):
        if "Top stories" in link.text.strip():  # Look for the exact text in the link
            href = link.get("href")
            return "https://news.google.com" + href[1:]  # Convert relative URL to full URL

    return None  # Return None if no valid link is found

def update_config_file(config_path, new_url):
    """Updates the 'url' field in the config.json file."""
    try:
        with open(config_path, "r", encoding="utf-8") as file:
            config = json.load(file)
        
        config["url"] = new_url  # Update the URL field

        with open(config_path, "w", encoding="utf-8") as file:
            json.dump(config, file, indent=4)

        print(f"\nUpdated config.json with URL: {new_url}")
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"\nError updating config.json: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape the 'Top Stories' link from a saved HTML file and update config.json")
    parser.add_argument("--file", type=str, default="google_news_homepage.html", help="Path to the saved Google News homepage HTML file")
    parser.add_argument("--config", type=str, default="config.json", help="Path to the config.json file")
    args = parser.parse_args()

    # Read the saved HTML file
    html_content = read_html_file(args.file)

    if html_content:
        top_stories_link = extract_top_stories_link(html_content)

        if top_stories_link:
            print(f"\nExtracted 'Top Stories' Link: {top_stories_link}")

            # Save to config.json
            update_config_file(args.config, top_stories_link)
        else:
            print("\nNo 'Top Stories' link found.")
    else:
        print("\nFailed to read the saved Google News homepage HTML file.")
