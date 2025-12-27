import sqlite3
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from queries_db_script import query_1, query_2, query_3

def main():
    conn = sqlite3.connect("localdb.sqlite")
    cursor = conn.cursor()

    # Show first 5 movies
    cursor.execute("SELECT * FROM movies LIMIT 1")
    for row in cursor.fetchall():
        print(row)

    # Query 1
    print("Query 1")
    print(query_1(cursor))

    # Query 2
    print("Query 2")
    print(query_2(cursor))

    # Query 3
    print("Query 3")
    print(query_3(cursor, "fr", "Action"))

    conn.close()


if __name__ == "__main__":
    main()