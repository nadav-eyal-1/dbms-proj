import mysql.connector
from mysql.connector import Error
import requests
import time
from create_db_script import get_connection

API_KEY = "71bfe53bf38ad02127239a25a12cc158"
BASE_URL = "https://api.themoviedb.org/3"

LANGUAGES = ["en", "fr", "es", "sv", "de"]
MOVIES_PER_LANGUAGE = 1000

LANGUAGE_NAMES = {
    "en": "English",
    "fr": "French",
    "es": "Spanish",
    "sv": "Swedish",
    "de": "German"
}


def fetch_discover(lang_code, page):
    """
    Fetch a page of popular movies filtered by original language.
    
    :param lang_code: str, ISO 639-1 language code (e.g., 'en', 'fr')
    :param page: int, page number to fetch
    :return: dict or None, JSON response containing movie results, or None if request fails
    """
    url = f"{BASE_URL}/discover/movie"
    params = {
        "api_key": API_KEY,
        "language": "en-US",
        "sort_by": "popularity.desc",
        "with_original_language": lang_code,
        "page": page
    }
    response = requests.get(url, params=params)
    return response.json() if response.status_code == 200 else None


def fetch_movie_full(movie_id):
    """
    Fetch full movie details including credits using append_to_response.
    
    :param movie_id: int, TMDB ID of the movie
    :return: dict or None, JSON response containing movie details and credits, or None if request fails
    """
    url = f"{BASE_URL}/movie/{movie_id}"
    params = {
        "api_key": API_KEY,
        "language": "en-US",
        "append_to_response": "credits"
    }
    response = requests.get(url, params=params)
    return response.json() if response.status_code == 200 else None


def insert_language(cursor, lang_code):
    """
    Insert a language into the 'languages' table if it does not already exist.
    
    :param cursor: MySQL cursor object
    :param lang_code: str, ISO 639-1 language code to insert
    :return: None
    """
    cursor.execute(
        "INSERT IGNORE INTO languages (lang_code, lang_name) VALUES (%s, %s)",
        (lang_code, LANGUAGE_NAMES.get(lang_code, lang_code))
    )


def main():
    """
    Main function to fetch movie data from TMDB and insert it into the database.
    Handles multiple languages, genres, and people.
    
    :return: None
    """
    try:
        con = get_connection()
        cursor = con.cursor()

        seen_genres = set()
        seen_people = set()

        for lang in LANGUAGES:
            print(f"\nCollecting movies for language: {lang}")
            insert_language(cursor, lang)

            inserted_count = 0
            page = 1

            while inserted_count < MOVIES_PER_LANGUAGE:
                print(f"fetching page number {page}")
                data = fetch_discover(lang, page)
                if not data or "results" not in data:
                    break

                movies_batch = []
                genres_batch = []
                movie_genres_batch = []
                people_batch = []
                movie_crew_batch = []

                for basic_movie in data["results"]:
                    if inserted_count >= MOVIES_PER_LANGUAGE:
                        break

                    movie_id = basic_movie["id"]
                    movie = fetch_movie_full(movie_id)
                    if not movie:
                        continue

                    release_year = movie.get("release_date")[:4] if movie.get("release_date") else None

                    movies_batch.append((
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

                    # Genres
                    for g in movie.get("genres", []):
                        if g["id"] not in seen_genres:
                            genres_batch.append((g["id"], g["name"]))
                            seen_genres.add(g["id"])
                        movie_genres_batch.append((movie_id, g["id"]))

                    # Crew
                    credits = movie.get("credits", {})
                    for crew in credits.get("crew", []):
                        if crew["id"] not in seen_people:
                            people_batch.append((
                                crew["id"],
                                crew["name"],
                                crew.get("popularity")
                            ))
                            seen_people.add(crew["id"])

                        movie_crew_batch.append((
                            movie_id,
                            crew["id"],
                            crew.get("job")
                        ))

                    inserted_count += 1
                    time.sleep(0.1)  

                # Execute batch inserts
                if movies_batch:
                    cursor.executemany("""
                        INSERT IGNORE INTO movies (
                            movie_id, title, original_title, tagline, overview,
                            release_year, runtime, popularity, vote_average,
                            vote_count, lang_code, budget
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, movies_batch)

                if genres_batch:
                    cursor.executemany(
                        "INSERT IGNORE INTO genres (genre_id, name) VALUES (%s, %s)",
                        genres_batch
                    )

                if people_batch:
                    cursor.executemany(
                        "INSERT IGNORE INTO people (person_id, name, popularity) VALUES (%s, %s, %s)",
                        people_batch
                    )


                if movie_genres_batch:
                    cursor.executemany(
                        "INSERT IGNORE INTO movie_genres (movie_id, genre_id) VALUES (%s, %s)",
                        movie_genres_batch
                    )

                if movie_crew_batch:
                    cursor.executemany("""
                        INSERT IGNORE INTO movie_crew (movie_id, person_id, job)
                        VALUES (%s, %s, %s)
                    """, movie_crew_batch)

                page += 1
                if page > data.get("total_pages", 100):
                    break

            print(f"Finished language {lang} with {inserted_count} movies")

        con.commit()
        print("\nAll data inserted successfully.")

    except Error as err:
        print(f"Database error: {err}")
        con.rollback()

    finally:
        if con.is_connected():
            con.close()


if __name__ == "__main__":
    main()
