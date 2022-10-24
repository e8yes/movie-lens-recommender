from kafka import KafkaProducer
from pyspark.sql import DataFrame, Row
from typing import Iterable

from src.ingestion.crawler.common import *
from src.ingestion.database.reader import *
from src.ingestion.proto_py.kafka_message_pb2 import ImdbEntry
from src.ingestion.proto_py.kafka_message_pb2 import TmdbEntry


def __UnpopulatedXmdbEntries(postgres_host: str,
                             postgres_password: str,
                             x: str) -> DataFrame:
    reader = IngestionReader(db_host=postgres_host,
                             db_user="postgres",
                             db_password=postgres_password)
    contents = ReadContents(reader)

    id_column = str()
    primary_info_column = str()

    if x == "imdb":
        id_column = CONTENT_DF_IMDB_ID
        primary_info_column = CONTENT_DF_IMDB_PRIMARY_INFO
    elif x == "tmdb":
        id_column = CONTENT_DF_TMDB_ID
        primary_info_column = CONTENT_DF_TMDB_PRIMARY_INFO
    else:
        raise "Unkown db type."

    return contents.\
        filter(contents[primary_info_column].isNull()).\
        filter(contents[id_column].isNotNull()).\
        select([id_column]).\
        distinct()


def __EnqueueXmdbEntries(entries: Iterable[Row],
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
    xmdb_ids = __UnpopulatedXmdbEntries(postgres_host=postgres_host,
                                        postgres_password=postgres_password,
                                        x=x)
    num_entries_scheduled = xmdb_ids.rdd. mapPartitions(
        lambda entries: __EnqueueXmdbEntries(entries, kafka_host, x)).\
        sum()
    return num_entries_scheduled
