import grpc
from typing import List
from pyspark import Row

from src.ingestion.proto_py.user_feedback_ingestion_service_pb2 import RatingFeedback
from src.ingestion.proto_py.user_feedback_ingestion_service_pb2 import TaggingFeedback
from src.ingestion.proto_py.user_feedback_ingestion_service_pb2 import RecordRatingFeedbacksRequest
from src.ingestion.proto_py.user_feedback_ingestion_service_pb2 import RecordTaggingFeedbacksRequest
from src.ingestion.proto_py.user_feedback_ingestion_service_pb2_grpc import UserFeedbackIngestionStub
from src.loader.uploader import RecordUploaderConnection
from src.loader.uploader import RecordUploaderInterface


class UserRatingUploader(RecordUploaderInterface):
    """Uploads user rating feedbacks to the user feedback ingestion service.
    """

    def __init__(self,
                 host: str,
                 col_name_user_id: str,
                 col_name_content_id: str,
                 col_name_timestamp: str,
                 col_name_rating: str) -> None:
        """Constructs a user rating feedback uploader.

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
        """Uploads user rating feedbacks in batch.
        """
        request = RecordRatingFeedbacksRequest()
        for row in batch:
            rating_feedback = RatingFeedback(
                user_id=row[self.col_name_user_id],
                content_id=row[self.col_name_content_id],
                timestamp_secs=row[self.col_name_timestamp],
                rating=row[self.col_name_rating])

            request.rating_feedbacks.append(rating_feedback)
        try:
            user_feedback_ingestion_stub: UserFeedbackIngestionStub = connection.stub
            user_feedback_ingestion_stub.RecordRatingFeedbacks(request)

            return True
        except grpc.RpcError as e:
            return False


class UserTaggingUploader(RecordUploaderInterface):
    """Uploads user tagging feedbacks to the user feedback ingestion service.
    """

    def __init__(self,
                 host: str,
                 col_name_user_id: str,
                 col_name_content_id: str,
                 col_name_timestamp: str,
                 col_name_tag: str) -> None:
        """Constructs a user tagging feedback uploader.

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
        self.col_name_tag = col_name_tag

    def MaxBatchSize(self) -> int:
        return 200

    def Connect(self) -> RecordUploaderConnection:
        """Connects to the user feedback ingestion service.
        """
        grpc_channel = grpc.insecure_channel(target=self.host)
        return RecordUploaderConnection(
            channel=grpc_channel, stub=UserFeedbackIngestionStub(grpc_channel))

    def UploadBatch(
            self, batch: List[Row],
            connection: RecordUploaderConnection) -> bool:
        """Uploads user tagging feedbacks in batch.
        """
        request = RecordTaggingFeedbacksRequest()
        for row in batch:
            tagging_feedback = TaggingFeedback(
                user_id=row[self.col_name_user_id],
                content_id=row[self.col_name_content_id],
                tag=row[self.col_name_tag],
                timestamp_secs=row[self.col_name_timestamp])

            request.tagging_feedbacks.append(tagging_feedback)

        try:
            user_feedback_ingestion_stub: UserFeedbackIngestionStub = connection.stub
            user_feedback_ingestion_stub.RecordTaggingFeedbacks(request)

            return True
        except grpc.RpcError as e:
            return False
