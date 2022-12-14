import grpc

from src.ingestion.database.common import UserRatingEntity
from src.ingestion.database.common import UserTaggingEntity
from src.ingestion.database.writer import IngestionWriterInterface
from src.ingestion.proto_py.user_feedback_ingestion_service_pb2 import \
    RecordRatingFeedbacksRequest
from src.ingestion.proto_py.user_feedback_ingestion_service_pb2 import \
    RecordRatingFeedbacksResponse
from src.ingestion.proto_py.user_feedback_ingestion_service_pb2 import \
    RecordTaggingFeedbacksRequest
from src.ingestion.proto_py.user_feedback_ingestion_service_pb2 import \
    RecordTaggingFeedbacksResponse
from src.ingestion.proto_py.user_feedback_ingestion_service_pb2_grpc import \
    UserFeedbackIngestionServicer


class UserFeedbackIngestionService(UserFeedbackIngestionServicer):
    """A service to support the recording of user feedback to the database.
    """

    def __init__(self, writer: IngestionWriterInterface) -> None:
        """Constructs a user feedback ingestion service.

        Args:
            writer (IngestionWriterInterface): An ingestion database writer.
        """
        super().__init__()
        self.writer = writer

    def RecordRatingFeedbacks(
            self, request: RecordRatingFeedbacksRequest,
            context: grpc.ServicerContext) -> RecordRatingFeedbacksResponse:
        """Records a list of user rating feedbacks to pieces of content. It
        overwrites any existing entries keyed by
        (user_id, content_id, timestamp_secs).

        Args:
            request (RecordRatingFeedbacksRequest): See
                user_feedback_ingestion_service.proto.

        Returns:
            RecordRatingFeedbacksResponse: See
                user_feedback_ingestion_service.proto.
        """
        to_be_written = list()
        for feedback in request.rating_feedbacks:
            entity = UserRatingEntity(user_id=feedback.user_id,
                                      content_id=feedback.content_id,
                                      timestamp_secs=feedback.timestamp_secs,
                                      rating=feedback.rating)
            to_be_written.append(entity)

        if self.writer.WriteUserRatings(ratings=to_be_written):
            context.set_code(grpc.StatusCode.OK)
        else:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        return RecordRatingFeedbacksResponse()

    def RecordTaggingFeedbacks(
            self, request: RecordTaggingFeedbacksRequest,
            context: grpc.ServicerContext) -> RecordTaggingFeedbacksResponse:
        """Records a list of user tagging feedbacks to pieces of content. It
        overwrites any existing entries keyed by
        (user_id, content_id, tag).

        Args:
            request (Record): See
                user_feedback_ingestion_service.proto.

        Returns:
            RecordRatingFeedbacksResponse: See
                user_feedback_ingestion_service.proto.
        """
        to_be_written = list()
        for feedback in request.tagging_feedbacks:
            entity = UserTaggingEntity(user_id=feedback.user_id,
                                       content_id=feedback.content_id,
                                       tag=feedback.tag,
                                       timestamp_secs=feedback.timestamp_secs)
            to_be_written.append(entity)

        if self.writer.WriteUserTaggings(tags=to_be_written):
            context.set_code(grpc.StatusCode.OK)
        else:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        return RecordTaggingFeedbacksResponse()
