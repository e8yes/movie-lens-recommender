import grpc
from typing import List
from pyspark import Row

from src.ingestion.proto_py.user_ingestion_service_pb2 import UserProfile, WriteUserProfilesRequest
from src.ingestion.proto_py.user_ingestion_service_pb2_grpc import UserIngestionStub
from src.loader.uploader import RecordUploaderConnection
from src.loader.uploader import RecordUploaderInterface


class UserProfileUploader(RecordUploaderInterface):
    """Uploads user profiles to the user profile ingestion service.
    """

    def __init__(self, host: str, col_name_user_id: str) -> None:
        """Constructs a user profile uploader.

        Args:
            host (str): The IP address and port which points to the user
                profile ingestion service.
            col_name_user_id (str): The name of the user ID column in the data
                frame that needs to be uploaded.
        """
        super().__init__()

        self.host = host
        self.col_name_user_id = col_name_user_id

    def MaxBatchSize(self) -> int:
        return 100

    def Connect(self) -> RecordUploaderConnection:
        """Connects to the user profile ingestion service.
        """
        grpc_channel = grpc.insecure_channel(target=self.host)
        return RecordUploaderConnection(channel=grpc_channel,
                                        stub=UserIngestionStub(grpc_channel))

    def UploadBatch(
            self, batch: List[Row],
            connection: RecordUploaderConnection) -> bool:
        """Uploads user profiles in batch.
        """
        try:
            request = WriteUserProfilesRequest()
            for row in batch:
                profile = UserProfile(user_id=row[self.col_name_user_id])
                request.user_profiles.append(profile)

            user_ingestion_stub: UserIngestionStub = connection.stub
            user_ingestion_stub.WriteUserProfiles(request)

            return True
        except:
            return False
