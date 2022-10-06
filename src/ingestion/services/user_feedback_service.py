import grpc
from typing import Any

from src.ingestion.database.common import UserFeedbackEntity
from src.ingestion.database.writer import WriteUserFeedbacks
from src.ingestion.proto_py.user_feedback_ingestion_service_pb2 import RecordRatingFeedbacksRequest
from src.ingestion.proto_py.user_feedback_ingestion_service_pb2 import RecordRatingFeedbacksResponse
from src.ingestion.proto_py.user_feedback_ingestion_service_pb2_grpc import UserFeedbackIngestionServicer


class UserFeedbackIngestionService(UserFeedbackIngestionServicer):
    """A service to support the recording of user feedback to the database.
    """

    def __init__(self, pg_conn: Any) -> None:
        """Constructs a user feedback ingestion service.

        Args:
            pg_conn (psycopg2.connection): A psycopg2 connection which connects
                to the ingestion database.
        """
        super().__init__()
        self.pg_conn = pg_conn

    def RecordRatingFeedbacks(
            self, request: RecordRatingFeedbacksRequest,
            context: grpc.ServicerContext) -> RecordRatingFeedbacksResponse:
        """Records a list of user feedbacks to pieces of content. It overwrites
        any existing entries keyed by (user_id, content_id).

        Args:
            request (RecordRatingFeedbacksRequest): See
                user_feedback_ingestion_service.proto.

        Returns:
            RecordRatingFeedbacksResponse: See
                user_feedback_ingestion_service.proto.
        """
        to_be_written = list()
        for feedback in request.rating_feedbacks:
            entity = UserFeedbackEntity(user_id=feedback.user_id,
                                        content_id=feedback.content_id,
                                        timestamp_secs=feedback.timestamp_secs,
                                        rating=feedback.rating)
            to_be_written.append(entity)

        if WriteUserFeedbacks(
                user_feedbacks=to_be_written, conn=self.pg_conn):
            context.set_code(grpc.StatusCode.OK)
        else:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        return RecordRatingFeedbacksResponse()