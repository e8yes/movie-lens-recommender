import json
from cassandra.cluster import Cluster
from cassandra.cluster import ResultSet
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from typing import List

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

SPARK_CASSANDRA_CONNECTOR_PATH = \
    "third_party/spark-cassandra-connector_2.12-3.2.0.jar"


def ConfigurePostgresSparkSession(
        contact_points: List[str],
        builder: SparkSession.Builder) -> SparkSession.Builder:
    """_summary_

    Args:
        contact_points (List[str]): The list of contact points to try
            connecting for cluster discovery. A contact point can be a string
            (ip or hostname), a tuple (ip/hostname, port) or a
            :class:`.connection.EndPoint` instance.
        builder (SparkSession.Builder): _description_

    Returns:
        SparkSession.Builder: _description_
    """
    return builder.\
        config("spark.jars", SPARK_CASSANDRA_CONNECTOR_PATH).\
        config("spark.sql.extensions",
               "com.datastax.spark.connector.CassandraSparkExtensions"). \
        config("spark.cassandra.connection.host",
               ",".join(contact_points))


class CassandraIngestionReader(IngestionReaderInterface):
    """A reader object which provides means to read data from the Cassandra
    ingestion database.
    """

    def __init__(self, contact_points: List[str], spark: SparkSession) -> None:
        """Constructs an ingestion DB reader object.

        Args:
            contact_points (List[str]): The list of contact points to try
                    connecting for cluster discovery. A contact point can be a
                    string (ip or hostname), a tuple (ip/hostname, port) or a
                    :class:`.connection.EndPoint` instance.
            spark (str): A spark session configured with
                ConfigureCassandraSparkSession().
        """
        super().__init__(reader_name="Cassandra")

        self.spark = spark

        self.cluster = Cluster(contact_points=contact_points)
        self.session = self.cluster.connect(INGESTION_DATABASE)

    def ReadTable(self, table_name: str) -> DataFrame:
        return self.spark.read.\
            format("org.apache.spark.sql.cassandra").\
            options(keyspace=INGESTION_DATABASE, table=table_name).\
            load()

    def ReadContentTmdbFields(
            self, content_id: int) -> TmdbContentProfileEntity:
        query = "SELECT                                                 \
                    {tmdb_id}, {primary_info}, {credits}, {keywords}    \
                 FROM {table}                                           \
                 WHERE {id} = %s".                                      \
            format(tmdb_id=CONTENT_PROFILE_TABLE_TMDB_ID,
                   primary_info=CONTENT_PROFILE_TABLE_TMDB_PRIMARY_INFO,
                   credits=CONTENT_PROFILE_TABLE_TMDB_CREDITS,
                   keywords=CONTENT_PROFILE_TABLE_TMDB_KEYWORDS,
                   table=CONTENT_PROFILE_TABLE,
                   id=CONTENT_PROFILE_TABLE_ID)

        result_set: ResultSet = self.session.execute(
            query=query, parameters=(content_id,))
        content_data = result_set.one()
        if content_data is None:
            return None

        content_data = result_set[0]
        return TmdbContentProfileEntity(
            tmdb_id=content_data[0],
            primary_info=json.loads(content_data[1])
            if content_data[1] is not None else None,
            credits=json.loads(content_data[2])
            if content_data[2] is not None else None,
            keywords=json.loads(content_data[3])
            if content_data[3] is not None else None)

    def ReadContentImdbFields(
            self, content_id: int) -> ImdbContentProfileEntity:
        query = "SELECT                                                 \
                    {imdb_id}, {primary_info}                           \
                 FROM {table}                                           \
                 WHERE {id} = %s".                                      \
            format(imdb_id=CONTENT_PROFILE_TABLE_IMDB_ID,
                   primary_info=CONTENT_PROFILE_TABLE_IMDB_PRIMARY_INFO,
                   table=CONTENT_PROFILE_TABLE,
                   id=CONTENT_PROFILE_TABLE_ID)

        result_set: ResultSet = self.session.execute(
            query=query, parameters=(content_id,))

        content_data = result_set.one()
        if content_data is None:
            return None

        imdb_id = content_data[0]
        primary_info_json = content_data[1]

        return ImdbContentProfileEntity(
            imdb_id=imdb_id,
            primary_info=json.loads(primary_info_json)
            if primary_info_json is not None else None)
