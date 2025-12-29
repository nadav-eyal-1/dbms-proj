import mysql.connector
from mysql.connector import Error

def get_connection():
    """
    Create and return a MySQL connection.
    """
    return mysql.connector.connect(
        host="localhost",
        user="nadaveyal1",
        password="12345",
        port=3305,
        database='nadaveyal1',
        autocommit=False
    )

def create_tables(cursor):
    """
    Create all tables.
    :param cursor: MySQL cursor
    :return: None
    """

    tables = {}

    tables["languages"] = """
        CREATE TABLE IF NOT EXISTS languages (
            lang_code VARCHAR(5) PRIMARY KEY,
            lang_name VARCHAR(50) NOT NULL
        )
    """

    tables["movies"] = """
        CREATE TABLE IF NOT EXISTS movies (
            movie_id INT PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            original_title VARCHAR(255),
            tagline VARCHAR(255),
            overview TEXT,
            release_year INT,
            runtime INT,
            popularity FLOAT,
            vote_average FLOAT,
            vote_count INT,
            lang_code VARCHAR(5),
            budget INT,
            FOREIGN KEY (lang_code) REFERENCES languages(lang_code)
        )
    """

    tables["genres"] = """
        CREATE TABLE IF NOT EXISTS genres (
            genre_id INT PRIMARY KEY,
            name VARCHAR(50) UNIQUE NOT NULL
        )
    """

    tables["movie_genres"] = """
        CREATE TABLE IF NOT EXISTS movie_genres (
            movie_id INT,
            genre_id INT,

            PRIMARY KEY (movie_id, genre_id),
            FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
            FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
        )
    """

    tables["people"] = """
        CREATE TABLE IF NOT EXISTS people (
            person_id INT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            popularity FLOAT
        )
    """

    tables["movie_crew"] = """
        CREATE TABLE IF NOT EXISTS movie_crew (
            movie_id INT,
            person_id INT,
            job VARCHAR(100),

            PRIMARY KEY (movie_id, person_id, job),
            FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
            FOREIGN KEY (person_id) REFERENCES people(person_id)
        )
    """

    for table_name, table_stmt in tables.items():
        try:
            print(f"Creating table: {table_name}")
            cursor.execute(table_stmt)
        except Error as err:
            print(f"Error while creating table {table_name}: {err}")
            raise


def create_indices(cursor):
    """
    Create indices after tables exist.
    :param cursor: MySQL cursor
    :return: None
    """

    indices = [
        (
            "idx_movies_lang_budget",
            """
            CREATE INDEX idx_movies_lang_budget
            ON movies (lang_code, budget);
            """
        ),
        (
            "idx_movie_crew_job",
            """
            CREATE INDEX idx_movie_crew_job
            ON movie_crew (job);
            """
        ),
        (
            "idx_movies_lang_vote",
            """
            CREATE INDEX idx_movies_lang_vote 
            ON movies(lang_code, vote_average);
            """
        ),
        (
            "idx_genres_name",
            """
            CREATE INDEX idx_genres_name 
            ON genres(name);
            """
        ),
        (
            "ft_idx_movie_content",
            """
            ALTER TABLE movies
            ADD FULLTEXT INDEX ft_idx_movie_content (title, overview, tagline)
            """
        )
    ]

    for name, stmt in indices:
        try:
            cursor.execute(stmt)
            print(f"Index created: {name}")
        except Error as err:
            print(f"Error while creating index {name}: {err}")


def main():
    """
    Connects to the database, creates tables and indices, and commits the schema.
    Handles errors and rolls back in case of failure.
    
    :return: None
    """
    try:
        con = get_connection()

        with con:
            cursor = con.cursor()
            try:
                cursor.execute("START TRANSACTION")

                create_tables(cursor)
                create_indices(cursor)

                con.commit()
                print("Database schema created successfully.")

            except Error as err:
                print(f"Failed creating schema: {err}")
                con.rollback()

    except Error as err:
        print(f"Connection error: {err}")

    finally:
        if 'con' in locals() and con.is_connected():
            con.close()


if __name__ == "__main__":
    main()
