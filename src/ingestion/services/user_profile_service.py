import grpc
from typing import Any

from src.ingestion.database.common import UserProfileEntity
from src.ingestion.database.writer import WriteUserProfiles
from src.ingestion.proto_py.user_ingestion_service_pb2 import WriteUserProfilesRequest
from src.ingestion.proto_py.user_ingestion_service_pb2 import WriteUserProfilesResponse
from src.ingestion.proto_py.user_ingestion_service_pb2_grpc import UserIngestionServicer


class UserIngestionService(UserIngestionServicer):
    """A service to support the recording of user metadata to the database.
    """

    def __init__(self, pg_conn: Any) -> None:
        """Constructs a user ingestion service.

        Args:
            pg_conn (psycopg2.connection): A psycopg2 connection which connects
                to the ingestion database.
        """
        super().__init__()
        self.pg_conn = pg_conn

    def WriteUserProfiles(
            self, request: WriteUserProfilesRequest,
            context: grpc.ServicerContext) -> WriteUserProfilesResponse:
        """It creates/updates a list of new user profiles, keyed by the user
        IDs. The call overwrites the profiles if the they have already been
        created in the system.

        Args:
            request (WriteUserProfilesRequest): See
                user_ingestion_service.proto.

        Returns:
            WriteUserProfilesResponse: See user_ingestion_service.proto.
        """
        to_be_written = list()
        for profile in request.user_profiles:
            entity = UserProfileEntity(user_id=profile.user_id)
            to_be_written.append(entity)

        WriteUserProfiles(user_profiles=to_be_written, conn=self.pg_conn)

        context.set_code(grpc.StatusCode.OK)
        return WriteUserProfilesResponse()
