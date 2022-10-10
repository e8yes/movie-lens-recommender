import grpc
from typing import List
from pyspark import Row

from src.ingestion.proto_py.user_feedback_ingestion_service_pb2 import RatingFeedback
from src.ingestion.proto_py.user_feedback_ingestion_service_pb2 import RecordRatingFeedbacksRequest
from src.ingestion.proto_py.user_feedback_ingestion_service_pb2_grpc import UserFeedbackIngestionStub
from src.loader.uploader import RecordUploaderConnection
from src.loader.uploader import RecordUploaderInterface


class UserFeedbackUploader(RecordUploaderInterface):
    """Uploads user feedbacks to the user feedback ingestion service.
    """

    def __init__(self,
                 host: str,
                 col_name_user_id: str,
                 col_name_content_id: str,
                 col_name_timestamp: str,
                 col_name_rating: str) -> None:
        """Constructs a user feedback uploader.

        Args:
            host (str): The IP address and port which points to the content
                profile ingestion service.
            col_name_content_id (str): The name of the content ID column in the
                data frame that needs to be uploaded.
        """
        super().__init__()

        self.host = host
        self.col_name_user_id = col_name_user_id
        self.col_name_content_id = col_name_content_id
        self.col_name_timestamp = col_name_timestamp
        self.col_name_rating = col_name_rating

    def MaxBatchSize(self) -> int:
        return 1000

    def Connect(self) -> RecordUploaderConnection:
        """Connects to the user feedback ingestion service.
        """
        grpc_channel = grpc.insecure_channel(target=self.host)
        return RecordUploaderConnection(
            channel=grpc_channel, stub=UserFeedbackIngestionStub(grpc_channel))

    def UploadBatch(
            self, batch: List[Row],
            connection: RecordUploaderConnection) -> bool:
        """Uploads user feedbacks in batch.
        """
        try:
            request = RecordRatingFeedbacksRequest()
            for row in batch:
                profile = RatingFeedback(
                    user_id=row[self.col_name_user_id],
                    content_id=row[self.col_name_content_id],
                    timestamp_secs=row[self.col_name_timestamp],
                    rating=row[self.col_name_rating])

                request.rating_feedbacks.append(profile)

            user_feedback_ingestion_stub: UserFeedbackIngestionStub = connection.stub
            user_feedback_ingestion_stub.RecordRatingFeedbacks(request)

            return True
        except grpc.RpcError as e:
            return False
