import requests
import json
import os
from dotenv import load_dotenv
from urllib.parse import urlencode, quote_plus
import sqlite3
from collections import defaultdict

load_dotenv()

API_TOKEN = os.getenv("ADS_DEV_KEY")
DEBUG = False

def build_search_url(query):
    '''Building url for accessing Search API to obtain paper bibcodes and fulltext'''
    base_url = "https://api.adsabs.harvard.edu/v1/search"
    encoded_query = urlencode(query)
    if DEBUG:
        print(encoded_query)
    results = requests.get("https://api.adsabs.harvard.edu/v1/search/query?{}".format(encoded_query), \
                       headers={'Authorization': 'Bearer ' + API_TOKEN})
    return results.json()

def build_metrics_url(bibcode_list):
    payload = {"bibcodes": bibcode_list,
          "types": ["histograms"],
          "histograms": ["citations"]}
    results = requests.post("https://api.adsabs.harvard.edu/v1/metrics", \
                        headers={'Authorization': 'Bearer ' + API_TOKEN, 
                                    "Content-type": "application/json"}, \
                        data=json.dumps(payload))
    response = results.json()

    skipped_bibcodes = response['skipped bibcodes']
    basic_stats = response['basic_stats']
    citation_stats = response['citation_stats']

def generate_query(search_query, fl):
    query = {"q": search_query,
             "fl": ",".join(fl),
             "fq":'database:astronomy',
             "sort": "date"}
    return query

def parse_search_response(response, fl):
    results_found = 0
    start_page = 0
    try:
        #response = response['response']
        results_found = response['response']['numFound']
        start_page = int(response['responseHeader']['params']['start'])
        docs = response['response']['docs']
    except Exception as e:
        print(f"Issue with parsing response: {e}")

    try:
        data_list = []
        for doc in docs:
            data = {}
            for field in fl:
                data[field] = doc.get(field, None)  # Use `.get` to handle missing fields
            data_list.append(data)

        if data_list:  # Proceed if there's data
            list_to_sqlite(data_list=data_list, db_path='astro.db', table_name='astro_papers')
    except Exception as e:
        print(f"Issue with processing docs: {e}")

    return results_found, start_page + len(docs)

def infer_sqlite_type(value):
    """
    Infers the SQLite type for a given value.
    """
    if isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "REAL"
    elif isinstance(value, (list, dict)):
        return "TEXT"  # Serialize lists/dicts as JSON strings
    else:
        return "TEXT"  # Default to TEXT for strings and other types

def list_to_sqlite(data_list, db_path, table_name):
    """
    Writes a list of dictionaries to an SQLite table.

    Args:
        data_list (list): A list of dictionaries to be inserted.
        db_path (str): Path to the SQLite database file.
        table_name (str): Name of the table to create/write to.
    """
    if not data_list:
        print("No data to write.")
        return

    # Extract column names and infer types dynamically
    columns = data_list[0].keys()
    column_types = {}

    for col in columns:
        for row in data_list:
            value = row.get(col)
            if value is not None:
                column_types[col] = infer_sqlite_type(value)
                break
        else:
            column_types[col] = "TEXT"  # Default to TEXT if all values are None

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table if it doesn't exist
    # create_table_sql = f"""
    #     CREATE TABLE IF NOT EXISTS {table_name} (
    #         {', '.join([f"{col} {column_types[col]}" for col in columns])}
    #     )
    # """
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join([f"{col} {column_types[col]}" for col in columns])},
        PRIMARY KEY (bibcode)
    )
    """

    cursor.execute(create_table_sql)

    # Serialize complex types (e.g., lists/dicts) as JSON strings
    for row in data_list:
        for col, value in row.items():
            if isinstance(value, (list, dict)):
                row[col] = json.dumps(value)

    # Insert data in batches
    # insert_sql = f"""
    #     INSERT INTO {table_name} ({', '.join(columns)}) 
    #     VALUES ({', '.join(['?' for _ in columns])})
    # """
    insert_sql = f"""
    INSERT OR IGNORE INTO {table_name} ({', '.join(columns)}) 
    VALUES ({', '.join(['?' for _ in columns])})
    """

    cursor.executemany(insert_sql, [tuple(row[col] for col in columns) for row in data_list])

    conn.commit()
    conn.close()

def main():
    fl = ['abstract', 'bibcode', 'alternate_bibcode', 'citation_count', 'date', \
          'pubdate', 'doi', 'page', 'read_count', 'title', 'year']
    query = generate_query("star clusters", fl)
    db_path = 'astro.db'
    table_name = 'astro_papers'

    start_page = 0  
    results_left = True

    start_page = 0  # Start pagination from 0
    results_found = 1  # Initialize to enter loop

    while start_page < results_found:
        query["start"] = start_page  # Update start page in the query
        try:
            response = build_search_url(query)  # Get API response
            results_found, next_start_page = parse_search_response(response, fl)
            start_page = next_start_page  # Update for the next iteration
        except Exception as e:
            print(f"Error processing page starting at {start_page}: {e}")
            break  # Exit the loop if there's an issue

if __name__ == "__main__":
    main()
