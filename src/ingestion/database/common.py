from json import JSONEncoder
from typing import Dict
from typing import List


# Database, table and column names.


INGESTION_DATABASE = "ingestion"

USER_PROFILE_TABLE = "user_profile"
USER_PROFILE_TABLE_ID = "id"
USER_PROFILE_TABLE_INGESTED_AT = "ingested_at"

CONTENT_PROFILE_TABLE = "content_profile"
CONTENT_PROFILE_TABLE_ID = "id"
CONTENT_PROFILE_TABLE_TITLE = "title"
CONTENT_PROFILE_TABLE_GENRES = "genres"
CONTENT_PROFILE_TABLE_SCORED_TAGS = "scored_tags"
CONTENT_PROFILE_TABLE_TAGS = "tags"
CONTENT_PROFILE_TABLE_IMDB_ID = "imdb_id"
CONTENT_PROFILE_TABLE_IMDB_PRIMARY_INFO = "imdb_primary_info"
CONTENT_PROFILE_TABLE_TMDB_ID = "tmdb_id"
CONTENT_PROFILE_TABLE_TMDB_PRIMARY_INFO = "tmdb_primary_info"
CONTENT_PROFILE_TABLE_TMDB_CREDITS = "tmdb_credits"
CONTENT_PROFILE_TABLE_TMDB_KEYWORDS = "tmdb_keywords"
CONTENT_PROFILE_TABLE_INGESTED_AT = "ingested_at"

USER_RATING_TABLE = "user_rating"
USER_RATING_TABLE_USER_ID = "user_id"
USER_RATING_TABLE_CONTENT_ID = "content_id"
USER_RATING_TABLE_RATING = "rating"
USER_RATING_TABLE_RATED_AT = "rated_at"
USER_RATING_TABLE_INGESTED_AT = "ingested_at"

USER_TAGGING_TABLE = "user_tagging"
USER_TAGGING_TABLE_USER_ID = "user_id"
USER_TAGGING_TABLE_CONTENT_ID = "content_id"
USER_TAGGING_TABLE_TAG = "tag"
USER_TAGGING_TABLE_RELEVANCE = "relevance"
USER_TAGGING_TABLE_TAGGED_AT = "tagged_at"
USER_TAGGING_TABLE_INGESTED_AT = "ingested_at"

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
                 tags: List[ContentTag],
                 imdb_id: int,
                 tmdb_id: int) -> None:
        self.content_id = content_id
        self.title = title
        self.genres = genres
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


class UserTaggingEntity:
    def __init__(self,
                 user_id: int,
                 content_id: int,
                 tag: str,
                 timestamp_secs: int) -> None:
        self.user_id = user_id
        self.content_id = content_id
        self.tag = tag
        self.timestamp_secs = timestamp_secs


class ImdbContentProfileEntity:
    def __init__(self,
                 imdb_id: int,
                 primary_info: Dict[str, any]) -> None:
        self.imdb_id = imdb_id
        self.primary_info = primary_info

    def __repr__(self) -> str:
        return "\
imdb_id={imdb_id} \
primary_info={primary_info}".\
            format(imdb_id=self.imdb_id,
                   primary_info=self.primary_info)


class TmdbContentProfileEntity:
    def __init__(self,
                 tmdb_id: int,
                 primary_info: Dict[str, any],
                 credits: Dict[str, any],
                 keywords: Dict[str, any]) -> None:
        self.tmdb_id = tmdb_id
        self.primary_info = primary_info
        self.credits = credits
        self.keywords = keywords

    def __repr__(self) -> str:
        return "\
tmdb_id={tmdb_id} \
primary_info={primary_info} \
credits={credits} \
keywords={keywords}".\
            format(tmdb_id=self.tmdb_id,
                   primary_info=self.primary_info,
                   credits=self.credits,
                   keywords=self.keywords)

# Util classes and functions


class StatelessClassJsonEncoder(JSONEncoder):
    """Encodes classes that don't contain external states into JSON.
    """

    def default(self, obj):
        return obj.__dict__
