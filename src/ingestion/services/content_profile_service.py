from re import S
import grpc
from typing import Any

from src.ingestion.database.common import ContentProfileEntity, ContentTag
from src.ingestion.database.writer import WriteContentProfiles
from src.ingestion.proto_py.content_ingestion_service_pb2 import WriteContentProfilesRequest
from src.ingestion.proto_py.content_ingestion_service_pb2 import WriteContentProfilesResponse
from src.ingestion.proto_py.content_ingestion_service_pb2_grpc import ContentIngestionServicer


class ContentIngestionService(ContentIngestionServicer):
    """A service to support the recording of content metadata to the database.
    """

    def __init__(self, pg_conn: Any) -> None:
        """Constructs a content ingestion service.

        Args:
            pg_conn (psycopg2.connection): A psycopg2 connection which connects
                to the ingestion database.
        """
        super().__init__()
        self.pg_conn = pg_conn

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

            genome_scores = dict()
            for k, v in profile.genome_scores.items():
                genome_scores[k] = v

            tags = list()
            for tag in profile.tags:
                tags.append(ContentTag(tag=tag.text,
                                       timestamp_secs=tag.timestamp_secs))

            # Builds content profile entity.
            entity = ContentProfileEntity(content_id=profile.content_id,
                                          title=profile.title,
                                          genres=genres,
                                          genome_scores=genome_scores,
                                          tags=tags,
                                          imdb_id=profile.imdb_id,
                                          tmdb_id=profile.tmdb_id)
            to_be_written.append(entity)

        # Stores content profiles into the database.
        WriteContentProfiles(content_profiles=to_be_written, conn=self.pg_conn)

        # TODO(Davis): Sends the content pieces to the Kafka queue to signal the crawler to retrieve external metadata.

        context.set_code(grpc.StatusCode.OK)
        return WriteContentProfilesResponse()
