from googlenewsdecoder import gnewsdecoder

# This is one of the exact URLs from your failed logs
test_url = "https://news.google.com/rss/articles/CBMimAFBVV95cUxNX0RPakluY3hodUxQdVNNT0R3dGlmMEhCYmRMRmtiMndSYTk3X0hZcHVYblV1cl9sUkQyWTgycnpZNlo2Z1BBUUgxakdZdWhLX0lDUnE0TnptUk5GLWpzdTdpRy1jYW1zcWZwcVJQX1E0UUhuMnd1VDdDTWtoNGh3ZmVOMXRiVHBZcEFrcW5UMnBIRnB3VlNFeg?oc=5"

print("Decrypting...")
result = gnewsdecoder(test_url)

if result.get("status"):
    print(f"SUCCESS! The real URL is: {result['decoded_url']}")
else:
    print("Failed to decode.")