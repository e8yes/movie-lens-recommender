import grpc
from kafka import KafkaProducer
from typing import List

from src.ingestion.crawler.common import KAFKA_TOPIC_IMDB
from src.ingestion.crawler.common import KAFKA_TOPIC_TMDB
from src.ingestion.database.common import ContentProfileEntity, ContentTag
from src.ingestion.database.writer import IngestionWriterInterface
from src.ingestion.proto_py.content_ingestion_service_pb2 import \
    WriteContentProfilesRequest
from src.ingestion.proto_py.content_ingestion_service_pb2 import \
    WriteContentProfilesResponse
from src.ingestion.proto_py.content_ingestion_service_pb2_grpc import \
    ContentIngestionServicer
from src.ingestion.proto_py.kafka_message_pb2 import TmdbEntry
from src.ingestion.proto_py.kafka_message_pb2 import ImdbEntry


def SignalMetadataCrawler(
        content_profiles: List[ContentProfileEntity],
        producer: KafkaProducer):
    tmdb_ids = set()
    imdb_ids = set()

    for content_profile in content_profiles:
        tmdb_id = content_profile.tmdb_id
        imdb_id = content_profile.imdb_id

        if tmdb_id is not None and tmdb_id not in tmdb_ids:
            entry = TmdbEntry(tmdb_id=tmdb_id)
            producer.send(topic=KAFKA_TOPIC_TMDB, value=entry)

            tmdb_ids.add(tmdb_id)

        if imdb_id is not None and imdb_id not in imdb_ids:
            entry = ImdbEntry(imdb_id=imdb_id)
            producer.send(topic=KAFKA_TOPIC_IMDB, value=entry)

            imdb_ids.add(imdb_id)


class ContentIngestionService(ContentIngestionServicer):
    """A service to support the recording of content metadata to the database.
    """

    def __init__(
            self, writer: IngestionWriterInterface, kafka_host: str) -> None:
        """Constructs a content ingestion service.

        Args:
            writer (IngestionWriterInterface): An ingestion database writer.
            kafka_host (str): The address which points to the Kafka server.
        """
        super().__init__()
        self.writer = writer
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=[kafka_host],
            value_serializer=lambda x: x.SerializeToString())

    def WriteContentProfiles(self,
                             request: WriteContentProfilesRequest,
                             context: grpc.ServicerContext) -> \
            WriteContentProfilesResponse:
        """It creates/updates a list of new content profiles, keyed by the
        content IDs. The call overwrites the profiles if the they have already
        been created in the system.

        Args:
            request (WriteContentProfilesRequest): See
                content_ingestion_service.proto.

        Returns:
            WriteContentProfilesResponse: See content_ingestion_service.proto.
        """
        to_be_written = list()
        for profile in request.content_profiles:
            # Turns gRPC objects into python objects.
            genres = list()
            for genre in profile.genres:
                genres.append(genre)

            tags = list()
            for tag in profile.tags:
                tags.append(ContentTag(tag=tag.text,
                                       timestamp_secs=tag.timestamp_secs))

            # Builds content profile entity.
            entity = ContentProfileEntity(content_id=profile.content_id,
                                          title=profile.title,
                                          genres=genres,
                                          tags=tags,
                                          imdb_id=profile.imdb_id,
                                          tmdb_id=profile.tmdb_id)
            to_be_written.append(entity)

        # Stores content profiles into the database.
        self.writer.WriteContentProfiles(contents=to_be_written)

        # Sends the content pieces to the Kafka queue to signal the crawler to
        # retrieve external metadata.
        # SignalMetadataCrawler(content_profiles=to_be_written,
        #                       producer=self.kafka_producer)

        context.set_code(grpc.StatusCode.OK)
        return WriteContentProfilesResponse()
