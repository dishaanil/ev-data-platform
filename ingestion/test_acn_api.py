import os
import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("ACN_API_TOKEN")
SITE = os.getenv("ACN_SITE", "caltech")

url = f"https://ev.caltech.edu/api/v1/sessions/{SITE}"
params = {
    "where": 'connectionTime>="Wed, 1 May 2019 00:00:00 GMT" and connectionTime<="Thu, 2 May 2019 00:00:00 GMT"',
    "pretty": ""
}

response = requests.get(url, params=params, auth=(TOKEN, ""))
response.raise_for_status()

data = response.json()

print("Top-level keys:", data.keys())
print("Meta:", data.get("_meta"))
print("Links:", data.get("_links"))
print("First item keys:", data["_items"][0].keys() if data.get("_items") else "No items")
print("First item sample:", data["_items"][0] if data.get("_items") else "No items")