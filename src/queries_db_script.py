def query_1(cursor, lang_code):
    """
    Returns the number of blockbuster movies in a given non-English language.
    A blockbuster is defined as a movie with a budget higher than the
    average budget of English-language movies.

    :param cursor: MySQL cursor object
    :param lang_code: ISO 639-1 language code (e.g., 'fr', 'de', 'sv')
    :return: List of tuples, each containing:
        (lang_code, lang_name, num_blockbusters)
    """
    sql = """
    SELECT l.lang_code,
           l.lang_name,
           COUNT(*) AS num_blockbusters
    FROM movies m
    JOIN languages l ON m.lang_code = l.lang_code
    WHERE m.lang_code = %s
      AND m.lang_code != 'en'
      AND m.budget IS NOT NULL
      AND m.budget > (
          SELECT AVG(budget)
          FROM movies
          WHERE lang_code = 'en'
            AND budget IS NOT NULL
      )
    GROUP BY l.lang_code, l.lang_name;
    """
    cursor.execute(sql, (lang_code, ))
    return cursor.fetchall()

def query_2(cursor, lang_code):
    """
    Returns the director(s) who directed the most movies in a given language.

    :param cursor: MySQL cursor object
    :param lang_code: ISO 639-1 language code (e.g., 'en', 'fr', 'sv')
    :return: List of tuples, each containing:
        (lang_code, lang_name, director_id, director_name, num_movies)
    """
    sql = """
    SELECT 
        lang_code,
        lang_name,
        director_id,
        director_name,
        num_movies
    FROM (
        SELECT 
            l.lang_code,
            l.lang_name,
            mc.person_id AS director_id,
            p.name AS director_name,
            COUNT(*) AS num_movies,
            MAX(COUNT(*)) OVER () AS max_movies_in_language
        FROM movie_crew mc
        JOIN movies m ON mc.movie_id = m.movie_id
        JOIN languages l ON m.lang_code = l.lang_code
        JOIN people p ON mc.person_id = p.person_id
        WHERE mc.job = 'Director'
          AND m.lang_code = %s
        GROUP BY l.lang_code, l.lang_name, mc.person_id, p.name
    ) AS t
    WHERE num_movies = max_movies_in_language;
    """
    cursor.execute(sql, (lang_code, ))
    return cursor.fetchall()


def query_3(cursor, lang_code, genre_name):
    """
    Returns the top-rated movie for a specific genre in a given language.

    :param cursor: MySQL cursor object
    :param lang_code: ISO 639-1 code of the language (e.g., 'en', 'fr')
    :param genre_name: Name of the genre (e.g., 'Action', 'Comedy')
    :return: List of tuples, each containing:
        (genre_name, movie_id, title, original_title, release_year, vote_average, lang_name)
    """
    sql = """
    SELECT 
        g.name AS genre_name,
        m.movie_id,
        m.title,
        m.original_title,
        m.release_year,
        m.vote_average,
        l.lang_name
    FROM genres g
    JOIN movie_genres mg ON g.genre_id = mg.genre_id
    JOIN movies m ON mg.movie_id = m.movie_id
    JOIN languages l ON m.lang_code = l.lang_code
    WHERE m.lang_code = %s
      AND g.name = %s
      AND m.vote_average = (
          SELECT MAX(m2.vote_average)
          FROM movies m2
          JOIN movie_genres mg2 ON m2.movie_id = mg2.movie_id
          JOIN genres g2 ON mg2.genre_id = g2.genre_id
          WHERE m2.lang_code = %s
            AND g2.name = %s
      )
    """
    params = (lang_code, genre_name, lang_code, genre_name)
    cursor.execute(sql, params)
    return cursor.fetchall()


def query_4(cursor):
    """
    Returns movies related to war or historical battles, while excluding comedies.

    :param cursor: MySQL cursor object
    :return: List of tuples, each containing:
        (title, overview, release_year)
    """
    sql = """
    SELECT
        m.title,
        m.overview,
        m.release_year
    FROM movies m
    JOIN movie_genres mg ON m.movie_id = mg.movie_id
    JOIN genres g ON mg.genre_id = g.genre_id
    WHERE
         MATCH(m.title, m.overview, m.tagline)
            AGAINST('war history battle' IN BOOLEAN MODE)
      AND g.name <> 'Comedy';
    """
    cursor.execute(sql)
    return cursor.fetchall()


def query_5(cursor):
    """
    Returns dark crime Scandinavian movies.

    :param cursor: MySQL cursor object
    :return: List of tuples, each containing:
        (title, overview, lang_name)
    """
    sql = """
    SELECT
        m.title,
        m.overview,
        l.lang_name
    FROM movies m
    JOIN languages l ON m.lang_code = l.lang_code
    WHERE
        MATCH(m.title, m.overview, m.tagline)
            AGAINST('dark crime bleak mystery -comedy' IN BOOLEAN MODE)
      AND m.lang_code = 'sv';
    """
    cursor.execute(sql)
    return cursor.fetchall()
