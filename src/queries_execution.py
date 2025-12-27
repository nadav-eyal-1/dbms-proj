from create_db_script import get_connection
from queries_db_script import (
    query_1,
    query_2,
    query_3,
    query_4,
    query_5
)


def main():
    """
    Executes example queries from queries_db_script and prints the results.

    :return: None
    """
    try:
        con = get_connection()
        cursor = con.cursor()

        print("\n--- Query 1: Blockbuster Movies in french ---")
        results = query_1(cursor, lang_code="fr")
        for row in results:
            print(row)

        print("\n--- Query 2: Top Directors in spanish ---")
        results = query_2(cursor, lang_code="es")
        for row in results:
            print(row)

        print("\n--- Query 3: Top-Rated Drama Movie in German ---")
        results = query_3(cursor, lang_code="de", genre_name="Drama")
        for row in results:
            print(row)

        print("\n--- Query 4: Historical War Movies (Excluding Comedy) ---")
        results = query_4(cursor)
        for row in results:
            print(row)

        print("\n--- Query 5: Dark Crime Scandinavian Movies ---")
        results = query_5(cursor)
        for row in results:
            print(row)

    except Exception as e:
        print(f"Error executing queries: {e}")

    finally:
        if 'con' in locals() and con.is_connected():
            con.close()


if __name__ == "__main__":
    main()
