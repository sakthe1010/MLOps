import json
import os
import time
from datetime import datetime, timedelta
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright

def load_config():
    """Loads configuration from a JSON file."""
    with open("config.json", "r") as f:
        return json.load(f)

def scroll_page(page, scroll_timeout):
    """Scrolls down the page to load all content dynamically."""
    print("Scrolling to load all content...")
    last_height = page.evaluate("document.body.scrollHeight")
    while True:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(scroll_timeout)
        new_height = page.evaluate("document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    print("Finished scrolling.")

def download_image(page, url, output_dir):
    """Downloads images from a given URL and saves them locally."""
    try:
        full_url = urljoin(page.url, url)
        response = page.request.get(full_url)
        if response.status == 200:
            filename = f"image_{abs(hash(full_url))}.jpg"
            filepath = os.path.join(output_dir, filename)
            with open(filepath, "wb") as f:
                f.write(response.body())
            return filepath
        return None
    except Exception as e:
        print(f"Failed to download {url}: {str(e)}")
        return None

def decode_text(text):
    """Decodes text if double-encoded, otherwise returns the original text."""
    try:
        return text.encode('latin1').decode('utf-8') if "\\u" in text else text
    except UnicodeEncodeError:
        return text

def extract_stories(page, selectors, output_dir):
    """Extracts news stories from the webpage."""
    stories = []
    articles = page.query_selector_all(selectors["story_container"])

    # Extract current date and time
    current_datetime = datetime.now()
    current_date = current_datetime.strftime("%Y-%m-%d")  # YYYY-MM-DD
    yesterday_date = (current_datetime - timedelta(days=1)).strftime("%Y-%m-%d")  # YYYY-MM-DD for yesterday
    scrape_time = current_datetime.strftime("%H:%M:%S")  # HH:MM:SS

    for idx, article in enumerate(articles):
        article.scroll_into_view_if_needed()
        page.wait_for_timeout(500)

        # Extract and decode the headline
        headline_elem = article.query_selector(selectors["headline"])
        raw_headline = headline_elem.inner_text().strip() if headline_elem else None
        if not raw_headline or raw_headline.lower() == "no headline":
            continue  # Skip if no headline

        headline = decode_text(raw_headline)

        # Extract URL
        article_url = None
        if "article_url" in selectors:
            url_elem = article.query_selector(selectors["article_url"])
            article_url = url_elem.get_attribute("href") if url_elem else None
            if article_url:
                article_url = urljoin(page.url, article_url)

        if not article_url or article_url.lower() == "no url":
            continue  # Skip if no valid URL

        # Extract image URL
        img_elem = article.query_selector(selectors["thumbnail"])
        if not img_elem:  # Skip articles without an image
            continue  

        srcset = img_elem.get_attribute("srcset") or ""
        img_url = srcset.split(",")[0].split(" ")[0] if srcset else img_elem.get_attribute("src")

        if not img_url:  # Double-check if img_url is empty
            continue  

        # Extract article date (Replace "Yesterday" with actual date)
        article_date = current_date  # Default to today's date
        if "article_date" in selectors:
            date_elem = article.query_selector(selectors["article_date"])
            article_date_raw = date_elem.inner_text().strip() if date_elem else None

            if article_date_raw:
                if "ago" in article_date_raw.lower():
                    article_date = current_date  # Replace relative time like "3 hours ago"
                elif "yesterday" in article_date_raw.lower():
                    article_date = yesterday_date  # Replace "Yesterday" with actual date
                else:
                    article_date = article_date_raw  # Keep valid dates as they are

        # Download and save image
        img_path = download_image(page, img_url, output_dir)
        if not img_path:  # Ensure the image was successfully downloaded
            continue  

        # Append valid story
        stories.append({
            "headline": headline,
            "url": article_url,
            "thumbnail_local_path": img_path,
            "article_date": article_date,  # Now contains only the date
            "scrape_timestamp": scrape_time  # Now contains only the time
        })
    
    return stories

def scrape_top_stories():
    """Main function to scrape Google News and save output."""
    config = load_config()
    output_dir = "downloaded_images"
    os.makedirs(output_dir, exist_ok=True)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=config["headless"])
        page = browser.new_page(user_agent=config["user_agent"])
        
        page.goto(config["url"], wait_until="networkidle")
        scroll_page(page, config["scroll_timeout"])
        
        stories = extract_stories(page, config["selectors"], output_dir)
        
        with open(config["output_file"], "w", encoding="utf-8") as f:
            json.dump(stories, f, indent=2, ensure_ascii=False)  # Ensure proper Unicode storage
        print(f"Saved {len(stories)} stories to {config['output_file']}")
        browser.close()

if __name__ == "__main__":
    scrape_top_stories()