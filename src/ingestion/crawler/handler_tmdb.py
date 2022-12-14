import json
import tmdbsimple as tmdb
from pyspark.sql import Row
from typing import Dict

from src.ingestion.crawler.common import KAFKA_TOPIC_TMDB
from src.ingestion.crawler.consumer import XmdbEntryHandlerInterface
from src.ingestion.database.common import TmdbContentProfileEntity
from src.ingestion.database.reader import CONTENT_DF_TMDB_ID
from src.ingestion.database.reader import CONTENT_DF_TMDB_PRIMARY_INFO
from src.ingestion.database.reader import CONTENT_DF_TMDB_CREDITS
from src.ingestion.database.reader import CONTENT_DF_TMDB_KEYWORDS
from src.ingestion.database.reader import IngestionReaderInterface
from src.ingestion.database.writer import IngestionWriterInterface
from src.ingestion.proto_py.kafka_message_pb2 import TmdbEntry


def Stale(field: Dict[str, any], current_tmdb_id: int):
    # Needs to check if the the field is up-to-date because TMDB ID might
    # change for the content, but the TMDB fields were filled using the old
    # TMDB ID.
    return field is None or field["id"] != current_tmdb_id


def StaleRow(row: Row) -> bool:
    """Checks if the TMDB fields in a content row is stale. A TMDB field is
    stale either if it's null, or its JSON content doesn't match to that
    referred to by the TMDB ID.

    Args:
        row (Row): A row read from the content_profile table.

    Returns:
        bool: Returns true if any of the TMDB fields are stale.
    """
    if row[CONTENT_DF_TMDB_ID] is None:
        return False

    current_tmdb_id = row[CONTENT_DF_TMDB_ID]

    if row[CONTENT_DF_TMDB_PRIMARY_INFO] is None or \
        Stale(field=json.loads(row[CONTENT_DF_TMDB_PRIMARY_INFO]),
              current_tmdb_id=current_tmdb_id):
        return True

    if row[CONTENT_DF_TMDB_CREDITS] is None or \
        Stale(field=json.loads(row[CONTENT_DF_TMDB_CREDITS]),
              current_tmdb_id=current_tmdb_id):
        return True

    if row[CONTENT_DF_TMDB_KEYWORDS] is None or \
        Stale(field=json.loads(row[CONTENT_DF_TMDB_KEYWORDS]),
              current_tmdb_id=current_tmdb_id):
        return True

    return False


def UpdateTmdbProfile(old_profile: TmdbContentProfileEntity,
                      tmdb_api_key: str) -> TmdbContentProfileEntity:
    tmdb.API_KEY = tmdb_api_key
    tmdb.REQUESTS_TIMEOUT = 30

    movie = tmdb.Movies(old_profile.tmdb_id)

    if Stale(field=old_profile.primary_info,
             current_tmdb_id=old_profile.tmdb_id):
        try:
            old_profile.primary_info = movie.info()
        except Exception:
            pass

    if Stale(field=old_profile.credits,
             current_tmdb_id=old_profile.tmdb_id):
        try:
            old_profile.credits = movie.credits()
        except Exception:
            pass

    if Stale(field=old_profile.keywords,
             current_tmdb_id=old_profile.tmdb_id):
        try:
            old_profile.keywords = movie.keywords()
        except Exception:
            pass

    new_profile = old_profile
    return new_profile


class TmdbEntryHandler(XmdbEntryHandlerInterface):
    """It handles the TMDB Kafka message. It gets the TMDB web responses
    according to the entry's tmdb_id, then it writes the result to the tmdb
    table.
    """

    def __init__(self, tmdb_api_key: str) -> None:
        self.tmdb_api_key = tmdb_api_key

    def Topic(self) -> str:
        return KAFKA_TOPIC_TMDB

    def EntryDeserializer(self, x: str) -> TmdbEntry:
        entry = TmdbEntry()
        entry.ParseFromString(x)
        return entry

    def ProcessEntry(self, entry: TmdbEntry,
                     ingestion_reader: IngestionReaderInterface,
                     ingestion_writer: IngestionWriterInterface) -> str:
        old_profile = ingestion_reader.ReadContentTmdbFields(
            content_id=entry.content_id)

        new_profile = UpdateTmdbProfile(old_profile=old_profile,
                                        tmdb_api_key=self.tmdb_api_key)

        ingestion_writer.WriteContentTmdbFields(
            content_id=entry.content_id, tmdb=new_profile)

        return new_profile.__repr__()[:50]
