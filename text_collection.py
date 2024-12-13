import requests
import json
import os
from dotenv import load_dotenv
from urllib.parse import urlencode, quote_plus

load_dotenv()

API_TOKEN = os.getenv("NASA_ADS_TOKEN")
DEBUG = False

def build_url():
    base_url = "https://api.adsabs.harvard.edu/v1/search"
    query = {"q": "author:martínez neutron star"}
    encoded_query = urlencode(query)
    if DEBUG:
        print(encoded_query)
    results = requests.get("https://api.adsabs.harvard.edu/v1/search/query?{}".format(encoded_query), \
                       headers={'Authorization': 'Bearer ' + API_TOKEN})
    response = results.json()
    print(response)

def parse_response(response):
    pass

def main():
    build_url()
    parse_response()

if __name__ == "__main__":
    main()
