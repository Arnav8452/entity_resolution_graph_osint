import os
import hashlib
import re
import urllib.parse
from bs4 import BeautifulSoup
from curl_cffi import requests
import trafilatura
from trafilatura.metadata import extract_metadata
from googlenewsdecoder import gnewsdecoder

# Root of the project
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def unroll_google_link(google_url):
    try:
        print(f"Decrypting Google Link: {google_url}")
        result = gnewsdecoder(google_url)
        
        if result.get("status"):
            real_url = result["decoded_url"]
            print(f"Successfully decrypted: {real_url}")
            return real_url
        else:
            print(f"Decryption failed, falling back to original: {result.get('message')}")
            return google_url
            
    except Exception as e:
        print(f"Decoder exception: {e}")
        return google_url

def check_for_updates(url: str, last_etag: str = None, last_modified: str = None) -> tuple[bool, str, str]:
    """
    Checks if a URL has new content by using a HEAD request.
    Returns (has_updated, new_etag, new_modified).
    """
    try:
        response = requests.head(url, impersonate="chrome120", timeout=10, verify=False)
        
        # Fallback to GET if HEAD is not allowed
        if response.status_code == 405:
            response = requests.get(url, impersonate="chrome120", timeout=10, verify=False)
            
        if response.status_code >= 400:
            print(f"Warning: Got status code {response.status_code} for {url}")
            # If we fail to fetch headers properly, assume we shouldn't update
            return False, None, None

        new_etag = response.headers.get("ETag")
        new_modified = response.headers.get("Last-Modified")
        
        if new_etag and new_etag == last_etag:
            return False, new_etag, new_modified
            
        if new_modified and new_modified == last_modified:
            return False, new_etag, new_modified
            
        return True, new_etag, new_modified

    except Exception as e:
        print(f"Error checking HEAD for {url}: {e}")
        return False, None, None

def chunk_by_words(text: str, max_words: int = 500) -> list[str]:
    words = text.split()
    chunks = []
    for i in range(0, len(words), max_words):
        chunk = " ".join(words[i:i + max_words])
        chunks.append(chunk)
    return chunks

def scrape_article(url: str) -> list[str]:
    """
    Returns list of extracted text chunks.
    """
    try:
        response = requests.get(url, impersonate="chrome120", timeout=15, verify=False)
        if response.status_code != 200:
            print(f"Failed to fetch {url}: Status {response.status_code}")
            return []
            
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        
        chunks = []
        # Check if it's a Live Blog (they usually have multiple <article> tags or a specific class)
        articles = soup.find_all('article')
        
        if len(articles) > 1:
            print(f"Live Feed detected at {url}. Splitting by semantic containers...")
            for update in articles:
                text = update.get_text(separator=' ', strip=True)
                if len(text) > 100: # Ignore empty or tiny containers
                    chunks.append(text)
        else:
            # Standard single article
            text = trafilatura.extract(html)
            if not text:
                print(f"Trafilatura could not extract text from {url}, falling back to soup...")
                text = soup.get_text(separator=' ', strip=True)
                
            if text:
                chunks = chunk_by_words(text, max_words=500)
                
        return chunks

    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None

if __name__ == "__main__":
    # Test the scraper
    test_url = "https://example.com"
    print(f"Testing scraper with {test_url}")
    
    is_updated, new_etag, new_mod = check_for_updates(test_url)
    print(f"Updated: {is_updated}, ETag: {new_etag}, Last-Modified: {new_mod}")
    
    if is_updated:
        text = scrape_article(test_url)
        if text:
            print("\nExtracted Text Snapshot:")
            print(text[:200] + "...")
