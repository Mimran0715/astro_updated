import sqlite3
import os

def create_table():
    ''' Creates table for paper collection. Only used once. '''
    try:
        conn = sqlite3.connect('database.db')
        cursor = conn.cursor()
        response = cursor.execute("CREATE TABLE movie(title, year, score)")
        print(f"Table successfully created.")
    except Exception as e:
        print(f"Could not create table. Error: {e}")

def main():
    conn = sqlite3.connect("tutorial.db")
    

if __name__ == "__main__":
    main()