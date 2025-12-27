import sqlite3
import requests
import time

# =========================
# TMDB CONFIGURATION
# =========================

API_KEY = "71bfe53bf38ad02127239a25a12cc158"
BASE_URL = "https://api.themoviedb.org/3"

LANGUAGES = ["en", "fr", "es", "sv", "de"]
MOVIES_PER_LANGUAGE = 50  # adjust for more movies

LANGUAGE_NAMES = {
    "en": "English",
    "fr": "French",
    "es": "Spanish",
    "sv": "Swedish",
    "de": "German"
}

DB_FILE = "localdb.sqlite"

# =========================
# DATABASE HELPERS
# =========================

def get_connection():
    return sqlite3.connect(DB_FILE)


def create_tables(conn):
    cursor = conn.cursor()
    
    # languages table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS languages (
            lang_code TEXT PRIMARY KEY,
            lang_name TEXT NOT NULL
        )
    """)
    
    # movies table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS movies (
            movie_id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            original_title TEXT,
            tagline TEXT,
            overview TEXT,
            release_year INTEGER,
            runtime INTEGER,
            popularity REAL,
            vote_average REAL,
            vote_count INTEGER,
            lang_code TEXT,
            budget INTEGER,
            FOREIGN KEY(lang_code) REFERENCES languages(lang_code)
        )
    """)
    
    # genres table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS genres (
            genre_id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        )
    """)
    
    # movie_genres table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS movie_genres (
            movie_id INTEGER,
            genre_id INTEGER,
            PRIMARY KEY(movie_id, genre_id),
            FOREIGN KEY(movie_id) REFERENCES movies(movie_id),
            FOREIGN KEY(genre_id) REFERENCES genres(genre_id)
        )
    """)
    
    # people table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS people (
            person_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            popularity REAL
        )
    """)
    
    # movie_cast table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS movie_cast (
            movie_id INTEGER,
            person_id INTEGER,
            character_name TEXT,
            cast_order INTEGER,
            PRIMARY KEY(movie_id, person_id),
            FOREIGN KEY(movie_id) REFERENCES movies(movie_id),
            FOREIGN KEY(person_id) REFERENCES people(person_id)
        )
    """)
    
    # movie_crew table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS movie_crew (
            movie_id INTEGER,
            person_id INTEGER,
            job TEXT,
            PRIMARY KEY(movie_id, person_id, job),
            FOREIGN KEY(movie_id) REFERENCES movies(movie_id),
            FOREIGN KEY(person_id) REFERENCES people(person_id)
        )
    """)
    
    conn.commit()


def insert_language(conn, lang_code):
    lang_name = LANGUAGE_NAMES.get(lang_code, lang_code)
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO languages (lang_code, lang_name) VALUES (?, ?)", (lang_code, lang_name))
    conn.commit()


def insert_movie(conn, movie):
    release_year = movie.get("release_date", "")[:4] if movie.get("release_date") else None
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR IGNORE INTO movies (
            movie_id, title, original_title, tagline, overview,
            release_year, runtime, popularity, vote_average,
            vote_count, lang_code, budget
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        movie["id"],
        movie.get("title"),
        movie.get("original_title"),
        movie.get("tagline"),
        movie.get("overview"),
        release_year,
        movie.get("runtime"),
        movie.get("popularity"),
        movie.get("vote_average"),
        movie.get("vote_count"),
        movie.get("original_language"),
        movie.get("budget")
    ))
    conn.commit()


def insert_genres(conn, movie_id, genres):
    cursor = conn.cursor()
    for g in genres:
        # Insert genre if not exists
        cursor.execute("INSERT OR IGNORE INTO genres (genre_id, name) VALUES (?, ?)", (g["id"], g["name"]))
        # Link movie to genre
        cursor.execute("INSERT OR IGNORE INTO movie_genres (movie_id, genre_id) VALUES (?, ?)", (movie_id, g["id"]))
    conn.commit()


def insert_people(conn, movie_id, people_list, job_type="cast"):
    cursor = conn.cursor()
    for person in people_list:
        cursor.execute("INSERT OR IGNORE INTO people (person_id, name, popularity) VALUES (?, ?, ?)",
                       (person["id"], person.get("name"), person.get("popularity")))
        if job_type == "cast":
            cursor.execute("INSERT OR IGNORE INTO movie_cast (movie_id, person_id, character_name, cast_order) VALUES (?, ?, ?, ?)",
                           (movie_id, person["id"], person.get("character"), person.get("order")))
        else:
            cursor.execute("INSERT OR IGNORE INTO movie_crew (movie_id, person_id, job) VALUES (?, ?, ?)",
                           (movie_id, person["id"], person.get("job")))
    conn.commit()


# =========================
# TMDB HELPERS
# =========================

def fetch_discover(lang_code, page):
    url = f"{BASE_URL}/discover/movie"
    params = {
        "api_key": API_KEY,
        "language": "en-US",
        "sort_by": "popularity.desc",
        "with_original_language": lang_code,
        "page": page
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching discover movies for {lang_code}, page {page}: {response.status_code}")
        return None


def fetch_movie_details(movie_id):
    url = f"{BASE_URL}/movie/{movie_id}"
    params = {
        "api_key": API_KEY,
        "language": "en-US",
        "append_to_response": "credits"  # get cast & crew
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching movie details {movie_id}: {response.status_code}")
        return None


# =========================
# MAIN SCRIPT
# =========================

def main():
    conn = get_connection()
    create_tables(conn)

    for lang in LANGUAGES:
        print(f"\nCollecting movies for language: {lang}")
        insert_language(conn, lang)

        movie_ids = []
        page = 1

        # Collect movies
        while len(movie_ids) < MOVIES_PER_LANGUAGE:
            data = fetch_discover(lang, page)
            if not data or "results" not in data:
                break

            for movie in data["results"]:
                if len(movie_ids) >= MOVIES_PER_LANGUAGE:
                    break
                movie_ids.append(movie["id"])

            page += 1
            if page > 100:
                break

        movie_ids = movie_ids[:MOVIES_PER_LANGUAGE]

        # Fetch details & populate tables
        for idx, movie_id in enumerate(movie_ids):
            print(f"Fetching movie details {idx+1}/{len(movie_ids)}")
            details = fetch_movie_details(movie_id)
            if not details:
                continue

            # Insert movie
            insert_movie(conn, details)

            # Insert genres
            if "genres" in details:
                insert_genres(conn, details["id"], details["genres"])

            # Insert cast
            if "credits" in details and "cast" in details["credits"]:
                cast_list = details["credits"]["cast"][:10]  # top 10 cast
                for i, c in enumerate(cast_list):
                    c["order"] = i
                insert_people(conn, details["id"], cast_list, job_type="cast")

            # Insert crew
            if "credits" in details and "crew" in details["credits"]:
                directors = [c for c in details["credits"]["crew"] if c["job"] == "Director"]
                insert_people(conn, details["id"], directors, job_type="crew")

            time.sleep(0.25)

    conn.close()
    print("\nLocal SQLite database populated successfully.")


if __name__ == "__main__":
    main()
