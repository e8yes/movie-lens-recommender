from kafka import KafkaProducer
from pyspark.sql import DataFrame, Row, SparkSession
from typing import Iterable

from src.ingestion.crawler.common import KAFKA_TOPIC_IMDB
from src.ingestion.crawler.common import KAFKA_TOPIC_TMDB
from src.ingestion.database.reader import CONTENT_DF_IMDB_ID
from src.ingestion.database.reader import CONTENT_DF_IMDB_PRIMARY_INFO
from src.ingestion.database.reader import CONTENT_DF_TMDB_ID
from src.ingestion.database.reader import CONTENT_DF_TMDB_PRIMARY_INFO
from src.ingestion.database.reader import CONTENT_DF_TMDB_CREDITS
from src.ingestion.database.reader import CONTENT_DF_TMDB_KEYWORDS
from src.ingestion.database.reader import IngestionReaderInterface
from src.ingestion.database.reader import ReadContents
from src.ingestion.database.reader_psql import ConfigurePostgresSparkSession
from src.ingestion.database.reader_psql import PostgresIngestionReader
from src.ingestion.proto_py.kafka_message_pb2 import ImdbEntry
from src.ingestion.proto_py.kafka_message_pb2 import TmdbEntry


def _CreateSparkSession() -> SparkSession:
    spark_builder = SparkSession.builder.appName("XMDB Populator")
    spark_builder = ConfigurePostgresSparkSession(builder=spark_builder)
    return spark_builder.getOrCreate()


def _UnpopulatedXmdbEntries(
        reader: IngestionReaderInterface, x: str) -> DataFrame:
    contents = ReadContents(reader)

    id_column = str()
    data_columns = (None, None, None)

    if x == "imdb":
        id_column = CONTENT_DF_IMDB_ID
        data_columns = (CONTENT_DF_IMDB_PRIMARY_INFO,
                        CONTENT_DF_IMDB_PRIMARY_INFO,
                        CONTENT_DF_IMDB_PRIMARY_INFO)
    elif x == "tmdb":
        id_column = CONTENT_DF_TMDB_ID
        data_columns = (CONTENT_DF_TMDB_PRIMARY_INFO,
                        CONTENT_DF_TMDB_CREDITS,
                        CONTENT_DF_TMDB_KEYWORDS)
    else:
        raise "Unkown db type."

    return contents.                                                    \
        filter(contents[data_columns[0]].isNull() |
               contents[data_columns[1]].isNull() |
               contents[data_columns[2]].isNull() |
               (contents[data_columns[0]] == "null") |
               (contents[data_columns[1]] == "null")).                  \
        filter(contents[id_column].isNotNull()).select([id_column]).    \
        distinct()


def _EnqueueXmdbEntries(entries: Iterable[Row],
                        kafka_host: str,
                        x: str) -> Iterable[int]:
    kafka_producer = KafkaProducer(
        bootstrap_servers=[kafka_host],
        value_serializer=lambda x: x.SerializeToString())

    entry_count = 0

    for entry in entries:
        if x == "imdb":
            kafka_producer.send(
                topic=KAFKA_TOPIC_IMDB,
                value=ImdbEntry(imdb_id=entry[CONTENT_DF_IMDB_ID]))
        elif x == "tmdb":
            kafka_producer.send(
                topic=KAFKA_TOPIC_TMDB,
                value=TmdbEntry(tmdb_id=entry[CONTENT_DF_TMDB_ID]))
        else:
            raise "Unkown db type."

        entry_count += 1

    yield entry_count


def PopulateXmdbEntries(postgres_host: str,
                        postgres_password: str,
                        kafka_host: str,
                        x: str) -> int:
    """Takes unpopulated IMDB/TMDB entries and pumps them into the associated
    Kafka queue, so the entires can get populated by the crawler, which
    consumes the queue.

    Args:
        postgres_host (str): The IP address which points to the postgres
            ingestion database server.
        postgres_password (str): The password of the postgres user.
        kafka_host (str): The host address (with port number) which points to
            the Kafka XMDB topics server.
        x (str): The type of XMDB to be populated. Value can be chosen from
            {imdb, tmdb}

    Returns:
        int: The number of XMDB entries found to be unpopulated.
    """
    spark = _CreateSparkSession()
    ingestion_reader = PostgresIngestionReader(db_host=postgres_host,
                                               db_user="postgres",
                                               db_password=postgres_password,
                                               spark=spark)
    xmdb_ids = _UnpopulatedXmdbEntries(reader=ingestion_reader, x=x)

    num_entries_scheduled = xmdb_ids.rdd. mapPartitions(
        lambda entries: _EnqueueXmdbEntries(entries, kafka_host, x)).   \
        sum()
    return num_entries_scheduled
