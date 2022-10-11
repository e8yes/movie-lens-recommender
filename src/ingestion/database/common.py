import psycopg2 as pg
from json import JSONEncoder
from typing import Any, Dict, List


# Database, table and column names.


INGESTION_DATABASE = "ingestion"

IMDB_TABLE = "imdb"
IMDB_TABLE_ID = "id"
IMDB_TABLE_PRIMARY_INFO = "primary_info"
IMDB_TABLE_INGESTED_AT = "ingested_at"

TMDB_TABLE = "tmdb"
TMDB_TABLE_ID = "id"
TMDB_TABLE_PRIMARY_INFO = "primary_info"
TMDB_TABLE_CREDITS = "credits"
TMDB_TABLE_INGESTED_AT = "ingested_at"

USER_PROFILE_TABLE = "user_profile"
USER_PROFILE_TABLE_ID = "id"
USER_PROFILE_TABLE_INGESTED_AT = "ingested_at"

CONTENT_PROFILE_TABLE = "content_profile"
CONTENT_PROFILE_TABLE_ID = "id"
CONTENT_PROFILE_TABLE_TITLE = "title"
CONTENT_PROFILE_TABLE_GENRES = "genres"
CONTENT_PROFILE_TABLE_GENOME_SCORES = "genome_scores"
CONTENT_PROFILE_TABLE_TAGS = "tags"
CONTENT_PROFILE_TABLE_IMDB_ID = "imdb_id"
CONTENT_PROFILE_TABLE_TMDB_ID = "tmdb_id"
CONTENT_PROFILE_TABLE_INGESTED_AT = "ingested_at"

USER_RATING_TABLE = "user_rating"
USER_RATING_TABLE_USER_ID = "user_id"
USER_RATING_TABLE_CONTENT_ID = "content_id"
USER_RATING_TABLE_RATING = "rating"
USER_RATING_TABLE_RATED_AT = "rated_at"
USER_RATING_TABLE_INGESTED_AT = "ingested_at"


# ORM entities.


class UserProfileEntity:
    def __init__(self, user_id: int, ingested_at: int = None) -> None:
        self.user_id = user_id
        self.ingested_at = ingested_at


class ContentTag:
    def __init__(self, tag: str, timestamp_secs: int) -> None:
        self.tag = tag
        self.timestamp_secs = timestamp_secs


class ContentProfileEntity:
    def __init__(self,
                 content_id: int,
                 title: str,
                 genres: List[str],
                 genome_scores: Dict[str, float],
                 tags: List[ContentTag],
                 imdb_id: int,
                 tmdb_id: int) -> None:
        self.content_id = content_id
        self.title = title
        self.genres = genres
        self.genome_scores = genome_scores
        self.tags = tags
        self.imdb_id = None if imdb_id == 0 else imdb_id
        self.tmdb_id = None if tmdb_id == 0 else tmdb_id


class UserRatingEntity:
    def __init__(self,
                 user_id: int,
                 content_id: int,
                 timestamp_secs: int,
                 rating: float) -> None:
        self.user_id = user_id
        self.content_id = content_id
        self.timestamp_secs = timestamp_secs
        self.rating = rating


class ImdbContentProfileEntity:
    def __init__(self,
                 imdb_id: int,
                 primary_info: Any,
                 ingested_at: int = None) -> None:
        self.imdb_id = imdb_id
        self.primary_info = primary_info
        self.ingested_at = ingested_at


class TmdbContentProfileEntity:
    def __init__(self,
                 tmdb_id: int,
                 primary_info: Any,
                 credits: Any,
                 ingested_at: int = None) -> None:
        self.tmdb_id = tmdb_id
        self.primary_info = primary_info
        self.credits = credits
        self.ingested_at = ingested_at

# Util classes and functions


class StatelessClassJsonEncoder(JSONEncoder):
    """Encodes classes that don't contain external states into JSON.
    """

    def default(self, obj):
        return obj.__dict__


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
