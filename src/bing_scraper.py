import urllib.parse
import xml.etree.ElementTree as ET
from curl_cffi import requests

def fetch_bing_news(query: str):
    """
    Fetches Bing News RSS for a specific query.
    Returns a list of articles containing title, link, and guid.
    """
    safe_query = urllib.parse.quote(query)
    url = f"https://www.bing.com/news/search?q={safe_query}&format=rss"
    articles = []
    
    try:
        response = requests.get(url, impersonate="chrome120", timeout=15, verify=False)
        if response.status_code != 200:
            print(f"Failed to fetch Bing News for '{query}' (Status: {response.status_code})")
            return articles
            
        root = ET.fromstring(response.text)
        for item in root.findall('.//item'):
            link = item.find('link')
            title = item.find('title')
            guid = item.find('guid')
            pubDate = item.find('pubDate')
            
            link_val = link.text if link is not None else None
            title_val = title.text if title is not None else "Unknown Bing Article"
            # Fallback to link if guid is missing
            guid_val = guid.text if guid is not None else link_val
            pubDate_val = pubDate.text if pubDate is not None else "Unknown Date"
            
            if link_val:
                articles.append({
                    "title": title_val,
                    "link": link_val,
                    "guid": guid_val,
                    "date": pubDate_val
                })
    except Exception as e:
        print(f"Error fetching Bing News for '{query}': {e}")
        
    return articles
