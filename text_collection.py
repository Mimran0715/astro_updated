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

def generate_search_terms():    
    """ Can update later with more sophisticated method to extract terms for 
    search queries. 
    """

    search_terms = [
    "dark matter distribution",
    "stellar nucleosynthesis",
    "supernova light curves",
    "exoplanet atmospheres",
    "gravitational wave detections",
    "galactic rotation curves",
    "globular cluster dynamics",
    "cosmic microwave background anisotropies",
    "protoplanetary disks",
    "high-redshift quasars"
    ]
    return search_terms

def build_search_url(query):
    '''Building url for accessing Search API to obtain paper bibcodes and fulltext'''
    base_url = "https://api.adsabs.harvard.edu/v1/search"
    encoded_query = urlencode(query)
    if DEBUG:
        print(encoded_query)
    results = requests.get("https://api.adsabs.harvard.edu/v1/search/query?{}".format(encoded_query), \
                       headers={'Authorization': 'Bearer ' + API_TOKEN})
    return results.json()

def generate_search_query(search_query, fl):
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
        print(f"Issue with parsing search response: {e}")

    try:
        data_list = []
        for doc in docs:
            data = {}
            for field in fl:
                data[field] = doc.get(field, None) 
            data_list.append(data)

        if data_list:  
            list_to_sqlite(data_list=data_list, db_path='astro.db', table_name='astro_papers')
    except Exception as e:
        print(f"Issue with processing docs: {e}")

    return results_found, start_page + len(docs)

def build_metrics_url(bibcode_list):
    payload = {"bibcodes": bibcode_list,
          "types": ["basic", "citations", "indicators"]}
          #"histograms": ["citations"]}
    results = requests.post("https://api.adsabs.harvard.edu/v1/metrics", \
                        headers={'Authorization': 'Bearer ' + API_TOKEN, 
                                    "Content-type": "application/json"}, \
                        data=json.dumps(payload))
    return results.json()

def process_metrics(bibcode_list, db_path):
    """
    Fetches metrics for a list of bibcodes and stores them in the database.

    Args:
        bibcode_list (list): List of bibcodes to fetch metrics for.
        db_path (str): Path to the SQLite database file.
    """
    metrics_response = build_metrics_url(bibcode_list)

    print(metrics_response)

    if "skipped bibcodes" in metrics_response:
        print(f"Skipped bibcodes: {metrics_response['skipped bibcodes']}")

    insert_metrics_into_db(metrics_response, db_path)

def insert_metrics_into_db(metrics_response, db_path, table_name='astro_metrics'):
    """
    Inserts metrics data into the SQLite database for a list of bibcodes.

    Args:
        metrics_response (dict): The JSON response from the metrics API.
        db_path (str): Path to the SQLite database file.
        table_name (str): Name of the metrics table to insert data into.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    insert_sql = f"""
        INSERT OR REPLACE INTO {table_name} (
            bibcode, total_reads, avg_reads, total_citations, h_index, g_index, tori
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    data_to_insert = []
    for bibcode, metrics in metrics_response.items():
        #print(bibcode)
        #return
        basic_stats = metrics.get('basic stats', {})
        citation_stats = metrics.get('citation stats', {})
        indicators = metrics.get('indicators', {})

        data_to_insert.append((
            bibcode,
            basic_stats.get('total number of reads'),
            basic_stats.get('average number of reads'),
            citation_stats.get('total number of citations'),
            indicators.get('h'),
            indicators.get('g'),
            indicators.get('tori'),
        ))

    cursor.executemany(insert_sql, data_to_insert)
    conn.commit()
    conn.close()

def get_bibcodes_from_db(db_path, table_name='astro_papers'):
    """
    Retrieves all bibcodes from the astro_papers table.

    Args:
        db_path (str): Path to the SQLite database file.
        table_name (str): Name of the table to query.

    Returns:
        list: A list of bibcodes.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT bibcode FROM {table_name}")
    bibcodes = [row[0] for row in cursor.fetchall()]
    conn.close()
    return bibcodes

def batch_bibcodes(bibcodes, batch_size=20):
    """
    Splits a list of bibcodes into batches.

    Args:
        bibcodes (list): List of bibcodes to batch.
        batch_size (int): Number of bibcodes per batch.

    Returns:
        list of lists: A list containing batches of bibcodes.
    """
    for i in range(0, len(bibcodes), batch_size):
        yield bibcodes[i:i + batch_size]

def test_bibcodes():
    db_path = 'astro.db'

    bibcodes = get_bibcodes_from_db(db_path)

    if not bibcodes:
        print("No bibcodes found in the database.")
        return

    for batch in batch_bibcodes(bibcodes, batch_size=20):  
        print(f"Processing batch: {batch}")
        try:
            #print(batch)
            process_metrics(batch, db_path)
        except Exception as e:
            print(f"Error processing batch {batch}: {e}")

def infer_sqlite_type(value):
    """
    Infers the SQLite type for a given value.
    """
    if isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "REAL"
    elif isinstance(value, (list, dict)):
        return "TEXT"  
    else:
        return "TEXT"  

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

    columns = data_list[0].keys()
    column_types = {}

    for col in columns:
        for row in data_list:
            value = row.get(col)
            if value is not None:
                column_types[col] = infer_sqlite_type(value)
                break
        else:
            column_types[col] = "TEXT"  

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join([f"{col} {column_types[col]}" for col in columns])},
        PRIMARY KEY (bibcode)
    )
    """
    cursor.execute(create_table_sql)

    for row in data_list:
        for col, value in row.items():
            if isinstance(value, (list, dict)):
                row[col] = json.dumps(value)

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
    
    search_terms = generate_search_terms()
    for term in search_terms: 
        query = generate_search_query(term, fl)
        db_path = 'astro.db'
        table_name = 'astro_papers'

        start_page = 0  
        results_found = 1  

        while start_page < results_found:
            query["start"] = start_page  
            try:
                response = build_search_url(query)  
                results_found, next_start_page = parse_search_response(response, fl)
                start_page = next_start_page 
            except Exception as e:
                print(f"Error processing page starting at {start_page}: {e}")
                break 

    test_bibcodes()

if __name__ == "__main__":
    test_bibcodes()
    #main()
