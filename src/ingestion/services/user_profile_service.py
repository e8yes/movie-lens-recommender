import grpc

from src.ingestion.database.common import UserProfileEntity
from src.ingestion.database.writer import IngestionWriterInterface
from src.ingestion.proto_py.user_ingestion_service_pb2 import \
    WriteUserProfilesRequest
from src.ingestion.proto_py.user_ingestion_service_pb2 import \
    WriteUserProfilesResponse
from src.ingestion.proto_py.user_ingestion_service_pb2_grpc import \
    UserIngestionServicer


class UserIngestionService(UserIngestionServicer):
    """A service to support the recording of user metadata to the database.
    """

    def __init__(self, writer: IngestionWriterInterface) -> None:
        """Constructs a user ingestion service.

        Args:
            writer (IngestionWriterInterface): An ingestion database writer.
        """
        super().__init__()
        self.writer = writer

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

        self.writer.WriteUserProfiles(users=to_be_written)

        context.set_code(grpc.StatusCode.OK)
        return WriteUserProfilesResponse()
