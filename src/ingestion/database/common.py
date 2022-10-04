import psycopg2 as pg

# Database, table and column names.
INGESTION_DATABASE = "ingestion"

IMDB_TABLE = "imdb_table"
IMDB_TABLE_ID = "id"
IMDB_TABLE_PRIMARY_INFO = "primary_info"
IMDB_TABLE_INGESTED_AT = "ingested_at"

TMDB_TABLE = "tmdb_table"
TMDB_TABLE_ID = "id"
TMDB_TABLE_PRIMARY_INFO = "primary_info"
TMDB_CREDITS = "credits"
TMDB_INGESTED_AT = "ingested_at"

USER_PROFILE_TABLE = "user_profile"
USER_PROFILE_TABLE_ID = "id"
USER_PROFILE_TABLE_INGESTED_AT = "ingested_at"

CONTENT_PROFILE_TABLE = "content_profile"
CONTENT_PROFILE_TABLE_ID = "id"
CONTENT_PROFILE_TABLE_TITLE = "title"
CONTENT_PROFILE_TABLE_GENRES = "genres"
CONTENT_PROFILE_TABLE_GENOME_SCORES = "genres"
CONTENT_PROFILE_TABLE_TAGS = "tags"
CONTENT_PROFILE_TABLE_IMDB_ID = "imdb_id"
CONTENT_PROFILE_TABLE_TMDB_ID = "tmdb_id"
CONTENT_PROFILE_TABLE_INGESTED_AT = "ingested_at"

USER_FEEDBACK_TABLE = "user_feedback"
USER_FEEDBACK_TABLE_USER_ID = "user_id"
USER_FEEDBACK_TABLE_CONTENT_ID = "content_id"
USER_FEEDBACK_TABLE_RATING = "rating"
USER_FEEDBACK_TABLE_RATED_AT = "rated_at"
USER_FEEDBACK_TABLE_INGESTED_AT = "ingested_at"


def CreateConnection(host: str, password: str):
    """Creates a postgresql connection to the ingestion database.

    Args:
        host (str): IP address of the database server.
        password (str): Password of the postgres user.

    Returns:
        pg.connection: A postgresql connection.
    """
    conn_params = "host='{host}' dbname='{dbname}'          \
                   user='postgres' password='{password}'".  \
        format(
            host=host,
            dbname=INGESTION_DATABASE,
            password=password
        )

    return pg.connect(conn_params)
