from typing import Any
import tmdbsimple as tmdb

from src.ingestion.crawler.common import KAFKA_TOPIC_TMDB
from src.ingestion.crawler.consumer import XmdbEntryHandlerInterface
from src.ingestion.database.common import TmdbContentProfileEntity
from src.ingestion.database.writer import TmdbContentProfileComplete
from src.ingestion.database.writer import WriteTmdbContentProfiles
from src.ingestion.proto_py.kafka_message_pb2 import TmdbEntry


def GetTmdbProfile(tmdb_id: int,
                   tmdb_api_key: str) -> TmdbContentProfileEntity:
    tmdb.API_KEY = tmdb_api_key
    tmdb.REQUESTS_TIMEOUT = 30

    movie = tmdb.Movies(tmdb_id)

    primary_info_response = None
    credits_response = None

    try:
        primary_info_response = movie.info()
    except:
        pass

    try:
        credits_response = movie.credits()
    except:
        pass

    return TmdbContentProfileEntity(tmdb_id=tmdb_id,
                                    primary_info=primary_info_response,
                                    credits=credits_response)


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

    def ProcessEntry(self, entry: TmdbEntry, pg_conn: Any) -> str:
        if TmdbContentProfileComplete(tmdb_id=entry.tmdb_id, conn=pg_conn):
            return None

        profile = GetTmdbProfile(tmdb_id=entry.tmdb_id,
                                 tmdb_api_key=self.tmdb_api_key)
        WriteTmdbContentProfiles(tmdb_profiles=[profile], conn=pg_conn)
        return profile.__repr__()[:50]
