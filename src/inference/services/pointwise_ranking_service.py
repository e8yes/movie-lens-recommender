import grpc
from cassandra.cluster import Cluster
from typing import List

from src.inference.proto_py.pointwise_ranking_service_pb2 import \
    PointwiseEstimatesRequest
from src.inference.proto_py.pointwise_ranking_service_pb2 import \
    PointwiseEstimatesResponse
from src.inference.proto_py.pointwise_ranking_service_pb2_grpc import \
    PointwiseRankingServicer


class PointwiseRankingService(PointwiseRankingServicer):
    """A service which supports producing pointwise ranking estimates.
    """

    def __init__(self,
                 model_path: str,
                 cassandra_contact_points: List[str]) -> None:
        """_summary_

        Args:
            model_path (str): _description_
            cassandra_contact_points (List[str]): _description_
        """
        super().__init__()

        self.cluster = Cluster(contact_points=cassandra_contact_points)
        self.session = self.cluster.connect("model")

    def ComputePointwiseEstimates(
            self,
            request: PointwiseEstimatesRequest,
            context: grpc.ServicerContext) -> PointwiseEstimatesResponse:
        return PointwiseEstimatesResponse(probs=[0.5])
