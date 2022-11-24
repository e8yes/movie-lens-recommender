from psycopg2 import connect
from pyspark.sql import DataFrame, SparkSession

from src.ingestion.database.common import INGESTION_DATABASE
from src.ingestion.database.common import CONTENT_PROFILE_TABLE
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_ID
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_IMDB_ID
from src.ingestion.database.common import \
    CONTENT_PROFILE_TABLE_IMDB_PRIMARY_INFO
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_TMDB_ID
from src.ingestion.database.common import \
    CONTENT_PROFILE_TABLE_TMDB_PRIMARY_INFO
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_TMDB_CREDITS
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_TMDB_KEYWORDS
from src.ingestion.database.common import ImdbContentProfileEntity
from src.ingestion.database.common import TmdbContentProfileEntity
from src.ingestion.database.reader import IngestionReaderInterface


PSQL_JDBC_DRIVER_PATH = "third_party/postgresql-42.5.0.jar"


def ConfigurePostgresSparkSession(
        builder: SparkSession.Builder) -> SparkSession.Builder:
    """_summary_

    Args:
        builder (SparkSession.Builder): _description_

    Returns:
        SparkSession.Builder: _description_
    """
    return builder                                      \
        .config("spark.jars", PSQL_JDBC_DRIVER_PATH)    \
        .config("spark.executor.memory", "2g")          \
        .config("spark.driver.memory", "2g")            \



class PostgresIngestionReader(IngestionReaderInterface):
    """A reader object which provides means to read data from the ingestion
    database. It maintains a Spark session which connects to the postgres
    ingestion database server through JDBC.
    """

    def __init__(
            self,
            db_host: str,
            db_user: str,
            db_password: str,
            spark: SparkSession) -> None:
        """Constructs an ingestion DB reader object.

        Args:
            db_host (str): The IP address (with port number) which points to
                the postgres server.
            db_user (str): The postgres user to use while accessing the
                ingestion database.
            db_password (str): The password of the postgres user.
            spark (str): A spark session configured with
                ConfigurePostgresSparkSession().
        """
        super().__init__(spark=spark, reader_name="PostgreSQL")

        self.db_host = db_host
        self.db_user = db_user
        self.db_password = db_password

        conn_params = "host='{host}' dbname='{dbname}'          \
                   user='{user}' password='{password}'".  \
            format(
                host=db_host,
                dbname=INGESTION_DATABASE,
                user=db_user,
                password=db_password
            )

        self.conn = connect(conn_params)

    def ReadTable(self, table_name: str) -> DataFrame:
        psql_url = "jdbc:postgresql://{host}/{db_name}".format(
            host=self.db_host, db_name=INGESTION_DATABASE)

        return super().spark.read                       \
            .format("jdbc")                             \
            .option("url", psql_url)                    \
            .option("dbtable", table_name)              \
            .option("user", self.db_user)               \
            .option("password", self.db_password)       \
            .option("driver", "org.postgresql.Driver")  \
            .load()

    def ReadContentTmdbFields(
            self, content_id: int) -> TmdbContentProfileEntity:
        query = "SELECT                                                 \
                    {tmdb_id}, {primary_info}, {credits}, {keywords}    \
                 FROM {table}                                           \
                 WHERE {id}={arg}".                                     \
            format(tmdb_id=CONTENT_PROFILE_TABLE_TMDB_ID,
                   primary_info=CONTENT_PROFILE_TABLE_TMDB_PRIMARY_INFO,
                   credits=CONTENT_PROFILE_TABLE_TMDB_CREDITS,
                   keywords=CONTENT_PROFILE_TABLE_TMDB_KEYWORDS,
                   table=CONTENT_PROFILE_TABLE,
                   id=CONTENT_PROFILE_TABLE_ID,
                   arg=content_id)

        cursor = self.conn.cursor()
        cursor.execute(query=query)

        result_set = cursor.fetchall()
        if len(result_set) == 0:
            return None

        content_data = result_set[0]
        return TmdbContentProfileEntity(tmdb_id=content_data[0],
                                        primary_info=content_data[1],
                                        credits=content_data[2],
                                        keywords=content_data[3])

    def ReadContentImdbFields(
            self, content_id: int) -> ImdbContentProfileEntity:
        query = "SELECT                         \
                    {imdb_id}, {primary_info}   \
                 FROM {table}                   \
                 WHERE {id}={arg}".             \
            format(imdb_id=CONTENT_PROFILE_TABLE_IMDB_ID,
                   primary_info=CONTENT_PROFILE_TABLE_IMDB_PRIMARY_INFO,
                   table=CONTENT_PROFILE_TABLE,
                   id=CONTENT_PROFILE_TABLE_ID,
                   arg=content_id)

        cursor = self.conn.cursor()
        cursor.execute(query=query)

        result_set = cursor.fetchall()
        if len(result_set) == 0:
            return None

        content_data = result_set[0]
        return ImdbContentProfileEntity(imdb_id=content_data[0],
                                        primary_info=content_data[1])
