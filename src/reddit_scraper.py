import requests
from datetime import datetime, timezone

def fetch_reddit_hot(subreddit: str, limit: int = 5):
    """
    Fetches top hot posts from a given Reddit JSON endpoint using standard 'requests'.
    Retrieves the title, selftext, and URL, and serializes them into standardized text.
    """
    url = f"https://www.reddit.com/r/{subreddit}/hot.json?limit={limit}"
    
    # A standard User-Agent format explicitly requested by Reddit's API rules
    headers = {"User-Agent": "python:client_side_aggregator:v1.0 (by /u/arnav_chandra)"}
    
    articles = []
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        posts = data.get("data", {}).get("children", [])
        
        for post in posts:
            post_data = post.get("data", {})
            
            # Skip moderation stickies
            if post_data.get("stickied", False):
                continue
                
            title = post_data.get("title", "")
            selftext = post_data.get("selftext", "")
            post_url = post_data.get("url", "")
            permalink = post_data.get("permalink", "")
            post_name = post_data.get("name", "")
            created_utc = post_data.get("created_utc")
            
            date_val = datetime.fromtimestamp(created_utc, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if created_utc else "Unknown Date"
            full_reddit_link = f"https://www.reddit.com{permalink}"
            
            # Compile into standardized layout for AI parsing
            raw_text = f"Title: {title}\n"
            if selftext:
                raw_text += f"Body: {selftext}\n"
            if post_url and not post_url.startswith("https://www.reddit.com"):
                raw_text += f"External Link Included: {post_url}\n"
                
            articles.append({
                "title": title,
                "text": raw_text.strip(),
                "source_url": full_reddit_link,
                "guid": post_name,
                "date": date_val
            })
            
    except Exception as e:
        print(f"Reddit Scraper Error for {subreddit}: {e}")
        
    return articles
