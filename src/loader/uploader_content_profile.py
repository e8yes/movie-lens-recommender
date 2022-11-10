import grpc
from typing import List
from pyspark import Row

from src.ingestion.proto_py.content_ingestion_service_pb2 import ContentProfile
from src.ingestion.proto_py.content_ingestion_service_pb2 import WriteContentProfilesRequest
from src.ingestion.proto_py.content_ingestion_service_pb2_grpc import ContentIngestionStub
from src.loader.uploader import RecordUploaderConnection
from src.loader.uploader import RecordUploaderInterface


class ContentProfileUploader(RecordUploaderInterface):
    """Uploads content profiles to the content profile ingestion service.
    """

    def __init__(self,
                 host: str,
                 col_name_content_id: str,
                 col_name_title: str,
                 col_name_genres: str,
                 col_name_scored_genome_tags: str,
                 col_name_tags: str,
                 col_name_imdb_id: str,
                 col_name_tmdb_id: str) -> None:
        """Constructs a content profile uploader.

        Args:
            host (str): The IP address and port which points to the content
                profile ingestion service.
            col_name_content_id (str): The name of the content ID column in the
                data frame that needs to be uploaded.
        """
        super().__init__()

        self.host = host
        self.col_name_content_id = col_name_content_id
        self.col_name_title = col_name_title
        self.col_name_genres = col_name_genres
        self.col_name_scored_genome_tags = col_name_scored_genome_tags
        self.col_name_tags = col_name_tags
        self.col_name_imdb_id = col_name_imdb_id
        self.col_name_tmdb_id = col_name_tmdb_id

    def MaxBatchSize(self) -> int:
        return 100

    def Connect(self) -> RecordUploaderConnection:
        """Connects to the content profile ingestion service.
        """
        grpc_channel = grpc.insecure_channel(target=self.host)
        return RecordUploaderConnection(channel=grpc_channel,
                                        stub=ContentIngestionStub(grpc_channel))

    def UploadBatch(
            self, batch: List[Row],
            connection: RecordUploaderConnection) -> bool:
        """Uploads content profiles in batch.
        """
        try:
            request = WriteContentProfilesRequest()
            for row in batch:
                tags = list()
                if row[self.col_name_tags] is not None:
                    for tag_str, timestamp in row[self.col_name_tags].items():
                        tag = ContentProfile.Tag(text=tag_str,
                                                 timestamp_secs=timestamp)
                        tags.append(tag)

                profile = ContentProfile(
                    content_id=row[self.col_name_content_id],
                    title=row[self.col_name_title],
                    genres=row[self.col_name_genres],
                    scored_tags=row[self.col_name_scored_genome_tags],
                    tags=tags,
                    imdb_id=row[self.col_name_imdb_id],
                    tmdb_id=row[self.col_name_tmdb_id])

                request.content_profiles.append(profile)

            content_ingestion_stub: ContentIngestionStub = connection.stub
            content_ingestion_stub.WriteContentProfiles(request)

            return True
        except grpc.RpcError as _:
            return False
