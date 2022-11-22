import json
import tmdbsimple as tmdb

from src.ingestion.crawler.common import KAFKA_TOPIC_TMDB
from src.ingestion.crawler.consumer import XmdbEntryHandlerInterface
from src.ingestion.database.common import TmdbContentProfileEntity
from src.ingestion.database.reader import IngestionReaderInterface
from src.ingestion.database.writer import IngestionWriterInterface
from src.ingestion.proto_py.kafka_message_pb2 import TmdbEntry


def Stale(json_field: str, current_tmdb_id: int):
    field = None
    if json_field is not None:
        field = json.loads(json_field)

    # FIXME: Need to check if the the field is up-to-date because TMDB ID
    # might change for the content, but the TMDB fields were filled using the
    # old TMDB ID.
    return field is None


def UpdateTmdbProfile(old_profile: TmdbContentProfileEntity,
                      tmdb_api_key: str) -> TmdbContentProfileEntity:
    tmdb.API_KEY = tmdb_api_key
    tmdb.REQUESTS_TIMEOUT = 30

    movie = tmdb.Movies(old_profile.tmdb_id)

    if Stale(json_field=old_profile.primary_info,
             current_tmdb_id=old_profile.tmdb_id):
        try:
            old_profile.primary_info = json.dumps(movie.info())
        except Exception:
            pass

    if Stale(json_field=old_profile.credits,
             current_tmdb_id=old_profile.tmdb_id):
        try:
            old_profile.credits = json.dumps(movie.credits())
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
