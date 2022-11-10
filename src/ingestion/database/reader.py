from asyncio import DatagramTransport
from pyspark.sql import DataFrame, SparkSession

from src.ingestion.database.common import *

# Dataframe column names
CONTENT_DF_ID = CONTENT_PROFILE_TABLE_ID
CONTENT_DF_TITLE = CONTENT_PROFILE_TABLE_TITLE
CONTENT_DF_GENRES = CONTENT_PROFILE_TABLE_GENRES
CONTENT_DF_SCORED_TAGS = CONTENT_PROFILE_TABLE_SCORED_TAGS
CONTENT_DF_TAGS = CONTENT_PROFILE_TABLE_TAGS
CONTENT_DF_IMDB_ID = CONTENT_PROFILE_TABLE_IMDB_ID
CONTENT_DF_TMDB_ID = CONTENT_PROFILE_TABLE_TMDB_ID
CONTENT_DF_IMDB_PRIMARY_INFO = "imdb_" + IMDB_TABLE_PRIMARY_INFO
CONTENT_DF_TMDB_PRIMARY_INFO = "tmdb_" + TMDB_TABLE_PRIMARY_INFO
CONTENT_DF_TMDB_CREDITS = "tmdb_" + TMDB_TABLE_CREDITS

USER_DF_ID = USER_PROFILE_TABLE_ID

RATING_FEEDBACK_DF_USER_ID = USER_RATING_TABLE_USER_ID
RATING_FEEDBACK_DF_CONTENT_ID = USER_RATING_TABLE_CONTENT_ID
RATING_FEEDBACK_DF_RATED_AT = USER_RATING_TABLE_RATED_AT
RATING_FEEDBACK_DF_RATING = USER_RATING_TABLE_RATING

TAGGING_FEEDBACK_DF_USER_ID = USER_TAGGING_TABLE_USER_ID
TAGGING_FEEDBACK_DF_CONTENT_ID = USER_TAGGING_TABLE_CONTENT_ID
TAGGING_FEEDBACK_DF_TAGGED_AT = USER_TAGGING_TABLE_TAGGED_AT
TAGGING_FEEDBACK_DF_TAG = USER_TAGGING_TABLE_TAG


class IngestionReader:
    """A reader object which provides means to read data from the ingestion
    database. It maintains a Spark session which connects to the postgres
    ingestion database server through JDBC.
    """

    def __init__(
            self,
            db_host: str,
            db_user: str,
            db_password: str,
            jdbc_driver_path: str = "third_party/postgresql-42.5.0.jar") -> None:
        """Constructs an ingestion DB reader object.

        Args:
            db_host (str): The IP address (with port number) which points to
                the postgres server.
            db_user (str): The postgres user to use while accessing the
                ingestion database.
            db_password (str): The password of the postgres user.
            jdbc_driver_path (str): Path to the postgres JDBC driver binary.
                Defaults to "third_party/postgresql-42.5.0.jar".
        """
        self.db_host = db_host
        self.db_user = db_user
        self.db_password = db_password

        self.spark_session = SparkSession           \
            .builder                                \
            .appName("Ingestion Reader")            \
            .config("spark.jars", jdbc_driver_path) \
            .config("spark.executor.memory", "3g")  \
            .config("spark.driver.memory", "3g")    \
            .getOrCreate()

    def ReadTable(self, table_name: str) -> DataFrame:
        """Reads an ingestion table as a Spark dataframe.

        Args:
            table_name (str): The name of the table in the ingestion database.

        Returns:
            DataFrame: A Spark Dataframe where it contains all the data records
                from the specified ingestion table.
        """
        psql_url = "jdbc:postgresql://{host}/{db_name}".format(
            host=self.db_host, db_name=INGESTION_DATABASE)

        return self.spark_session.read                  \
            .format("jdbc")                             \
            .option("url", psql_url)                    \
            .option("dbtable", table_name)              \
            .option("user", self.db_user)               \
            .option("password", self.db_password)       \
            .option("driver", "org.postgresql.Driver")  \
            .load()


def ReadContents(reader: IngestionReader) -> DataFrame:
    """Reads data records from content profile related ingestion tables.

    Args:
        reader (IngestionReader): An ingestion DB reader.

    Returns:
        DataFrame: The schema is as follows,
            root
                |-- id: long (nullable = false)
                |-- title: string (nullable = true)
                |-- genres: array (nullable = true)
                |    |-- element: string (containsNull = false)
                |-- scored_tags: json (nullable = true)
                |-- tags: json (nullable = true)
                |-- imdb_id: integer (nullable = true)
                |-- tmdb_id: integer (nullable = true)
                |-- imdb_primary_info: json (nullable = true)
                |-- tmdb_primary_info: json (nullable = true)
                |-- tmdb_credits: json (nullable = true)
    """
    content = reader.ReadTable(
        table_name=CONTENT_PROFILE_TABLE)
    imdb = reader.ReadTable(table_name=IMDB_TABLE)
    tmdb = reader.ReadTable(table_name=TMDB_TABLE)

    content = content.drop(CONTENT_PROFILE_TABLE_INGESTED_AT)
    imdb = imdb.drop(IMDB_TABLE_INGESTED_AT)
    tmdb = tmdb.drop(TMDB_TABLE_INGESTED_AT)

    content = content.\
        join(other=imdb,
             on=content[CONTENT_PROFILE_TABLE_IMDB_ID] ==
             imdb[IMDB_TABLE_ID],
             how="leftouter").\
        drop(imdb[IMDB_TABLE_ID]).\
        withColumnRenamed(existing=IMDB_TABLE_PRIMARY_INFO,
                          new=CONTENT_DF_IMDB_PRIMARY_INFO)

    content = content.\
        join(other=tmdb,
             on=content[CONTENT_PROFILE_TABLE_TMDB_ID] ==
             tmdb[TMDB_TABLE_ID],
             how="leftouter").\
        drop(tmdb[TMDB_TABLE_ID]).\
        withColumnRenamed(existing=TMDB_TABLE_PRIMARY_INFO,
                          new=CONTENT_DF_TMDB_PRIMARY_INFO).\
        withColumnRenamed(existing=TMDB_TABLE_CREDITS,
                          new=CONTENT_DF_TMDB_CREDITS)

    return content


def ReadUsers(reader: IngestionReader) -> DataFrame:
    """Reads data records from user profile related ingestion tables.

    Args:
        reader (IngestionReader): An ingestion DB reader.

    Returns:
        DataFrame: The schema is as follows,
            root
                |-- id: long (nullable = false)
    """
    user = reader.ReadTable(table_name=USER_PROFILE_TABLE)
    user = user.drop(USER_PROFILE_TABLE_INGESTED_AT)
    return user


def ReadRatingFeedbacks(reader: IngestionReader) -> DataFrame:
    """Reads data records from user rating related ingestion tables.

    Args:
        reader (IngestionReader): An ingestion DB reader.

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


def ReadTaggingFeedbacks(reader: IngestionReader) -> DataFrame:
    """Reads data records from user tagging related ingestion tables.

    Args:
        reader (IngestionReader): An ingestion DB reader.

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
