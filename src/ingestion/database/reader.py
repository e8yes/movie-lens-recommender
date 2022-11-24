from pyspark.sql import DataFrame

from src.ingestion.database.common import CONTENT_PROFILE_TABLE
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_ID
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_TITLE
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_GENRES
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_TAGS
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_IMDB_ID
from src.ingestion.database.common import \
    CONTENT_PROFILE_TABLE_IMDB_PRIMARY_INFO
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_TMDB_ID
from src.ingestion.database.common import \
    CONTENT_PROFILE_TABLE_TMDB_PRIMARY_INFO
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_TMDB_CREDITS
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_TMDB_KEYWORDS
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_INGESTED_AT
from src.ingestion.database.common import USER_PROFILE_TABLE
from src.ingestion.database.common import USER_PROFILE_TABLE_ID
from src.ingestion.database.common import USER_PROFILE_TABLE_INGESTED_AT
from src.ingestion.database.common import USER_RATING_TABLE
from src.ingestion.database.common import USER_RATING_TABLE_USER_ID
from src.ingestion.database.common import USER_RATING_TABLE_CONTENT_ID
from src.ingestion.database.common import USER_RATING_TABLE_RATED_AT
from src.ingestion.database.common import USER_RATING_TABLE_RATING
from src.ingestion.database.common import USER_RATING_TABLE_INGESTED_AT
from src.ingestion.database.common import USER_TAGGING_TABLE
from src.ingestion.database.common import USER_TAGGING_TABLE_USER_ID
from src.ingestion.database.common import USER_TAGGING_TABLE_CONTENT_ID
from src.ingestion.database.common import USER_TAGGING_TABLE_TAGGED_AT
from src.ingestion.database.common import USER_TAGGING_TABLE_TAG
from src.ingestion.database.common import USER_TAGGING_TABLE_RELEVANCE
from src.ingestion.database.common import USER_TAGGING_TABLE_INGESTED_AT
from src.ingestion.database.common import ImdbContentProfileEntity
from src.ingestion.database.common import TmdbContentProfileEntity


# Dataframe column names
CONTENT_DF_ID = CONTENT_PROFILE_TABLE_ID
CONTENT_DF_TITLE = CONTENT_PROFILE_TABLE_TITLE
CONTENT_DF_GENRES = CONTENT_PROFILE_TABLE_GENRES
CONTENT_DF_TAGS = CONTENT_PROFILE_TABLE_TAGS
CONTENT_DF_IMDB_ID = CONTENT_PROFILE_TABLE_IMDB_ID
CONTENT_DF_IMDB_PRIMARY_INFO = CONTENT_PROFILE_TABLE_IMDB_PRIMARY_INFO
CONTENT_DF_TMDB_ID = CONTENT_PROFILE_TABLE_TMDB_ID
CONTENT_DF_TMDB_PRIMARY_INFO = CONTENT_PROFILE_TABLE_TMDB_PRIMARY_INFO
CONTENT_DF_TMDB_CREDITS = CONTENT_PROFILE_TABLE_TMDB_CREDITS
CONTENT_DF_TMDB_KEYWORDS = CONTENT_PROFILE_TABLE_TMDB_KEYWORDS

USER_DF_ID = USER_PROFILE_TABLE_ID

RATING_FEEDBACK_DF_USER_ID = USER_RATING_TABLE_USER_ID
RATING_FEEDBACK_DF_CONTENT_ID = USER_RATING_TABLE_CONTENT_ID
RATING_FEEDBACK_DF_RATED_AT = USER_RATING_TABLE_RATED_AT
RATING_FEEDBACK_DF_RATING = USER_RATING_TABLE_RATING

TAGGING_FEEDBACK_DF_USER_ID = USER_TAGGING_TABLE_USER_ID
TAGGING_FEEDBACK_DF_CONTENT_ID = USER_TAGGING_TABLE_CONTENT_ID
TAGGING_FEEDBACK_DF_TAGGED_AT = USER_TAGGING_TABLE_TAGGED_AT
TAGGING_FEEDBACK_DF_TAG = USER_TAGGING_TABLE_TAG
TAGGING_FEEDBACK_DF_RELEVANCE = USER_TAGGING_TABLE_RELEVANCE


class IngestionReaderInterface:
    """A reader object which provides means to access data in the ingestion
    database.
    """

    def __init__(self, reader_name: str) -> None:
        """Constructs an ingestion DB reader object.

        Args:
            reader_name (str): A human readable name of the reader
                implementation for debugging purposes.
        """
        self.reader_name = reader_name

    def ReadTable(self, table_name: str) -> DataFrame:
        """Reads an ingestion table specified by the table_name as a Spark
        data frame.

        Args:
            table_name (str): The name of the table in the ingestion database.

        Returns:
            DataFrame: A Spark Dataframe where it contains all the data records
                from the specified ingestion table.
        """
        pass

    def ReadContentTmdbFields(
            self, content_id: int) -> TmdbContentProfileEntity:
        """Retrieves TMDB fields of the piece of content specified by the
        content_id.

        Args:
            content_id (int): The piece of content where TMDB fields need to
                be retrieved.

        Returns:
            TmdbContentProfileEntity: TMDB Fields.
        """
        pass

    def ReadContentImdbFields(
            self, content_id: int) -> ImdbContentProfileEntity:
        """Retrieves IMDB fields of the piece of content specified by the
        content_id.

        Args:
            content_id (int): The piece of content where IMDB fields need to
                be retrieved.

        Returns:
            ImdbContentProfileEntity: IMDB Fields.
        """
        pass


def ReadContents(reader: IngestionReaderInterface) -> DataFrame:
    """Reads data records from content profile related ingestion tables.

    Args:
        reader (IngestionReaderInterface): An ingestion DB reader.

    Returns:
        DataFrame: The schema is as follows,
            root
                |-- id: long (nullable = false)
                |-- title: string (nullable = true)
                |-- genres: array (nullable = true)
                |    |-- element: string (containsNull = false)
                |-- tags: json (nullable = true)
                |-- imdb_id: integer (nullable = true)
                |-- tmdb_id: integer (nullable = true)
                |-- imdb_primary_info: json (nullable = true)
                |-- tmdb_primary_info: json (nullable = true)
                |-- tmdb_credits: json (nullable = true)
                |-- tmdb_keywords: array (nullable = true)
                |    |-- element: string (containsNull = false)
    """
    content = reader.ReadTable(
        table_name=CONTENT_PROFILE_TABLE)
    content.drop(CONTENT_PROFILE_TABLE_INGESTED_AT)
    return content


def ReadUsers(reader: IngestionReaderInterface) -> DataFrame:
    """Reads data records from user profile related ingestion tables.

    Args:
        reader (IngestionReaderInterface): An ingestion DB reader.

    Returns:
        DataFrame: The schema is as follows,
            root
                |-- id: long (nullable = false)
    """
    user = reader.ReadTable(table_name=USER_PROFILE_TABLE)
    user = user.drop(USER_PROFILE_TABLE_INGESTED_AT)
    return user


def ReadRatingFeedbacks(reader: IngestionReaderInterface) -> DataFrame:
    """Reads data records from user rating related ingestion tables.

    Args:
        reader (IngestionReaderInterface): An ingestion DB reader.

    Returns:
        DataFrame: The schema is as follows,
            root
                |-- user_id: long (nullable = false)
                |-- content_id: long (nullable = false)
                |-- rated_at: timestamp (nullable = false)
                |-- rating: double (nullable = false)
    """
    rating = reader.ReadTable(table_name=USER_RATING_TABLE)
    rating = rating.drop(USER_RATING_TABLE_INGESTED_AT)
    return rating


def ReadTaggingFeedbacks(reader: IngestionReaderInterface) -> DataFrame:
    """Reads data records from user tagging related ingestion tables.

    Args:
        reader (IngestionReaderInterface): An ingestion DB reader.

    Returns:
        DataFrame: The schema is as follows,
            root
                |-- user_id: long (nullable = false)
                |-- content_id: long (nullable = false)
                |-- tag: string (nullable = false)
                |-- tagged_at: timestamp (nullable = false)
    """
    tagging = reader.ReadTable(table_name=USER_TAGGING_TABLE)
    tagging = tagging.drop(USER_TAGGING_TABLE_INGESTED_AT)
    return tagging
