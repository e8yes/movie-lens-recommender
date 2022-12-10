import grpc
import numpy as np
import tensorflow as tf
from cassandra.cluster import Cluster
from typing import Dict
from typing import List

from src.model.reader import sql_builder
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

        self.model = tf.keras.models.load_model(model_path)

    def _UserIdsAsNumpy(
            self, request: PointwiseEstimatesRequest) -> np.ndarray:
        user_ids = np.array(request.user_ids)
        return np.reshape(a=user_ids, newshape=(-1, 1))

    def _FeatureTable(
            self,
            ids: List[int],
            table_name: str) -> Dict[int, List[float]]:
        sql = sql_builder(set(ids), table_name)
        feature_rows = self.session.execute(sql)

        dic = dict()
        for x in feature_rows:
            dic.update({x[0]: x[1]})

        return dic

    def _UserFeatures(self, request: PointwiseEstimatesRequest) -> np.ndarray:
        user_dic = self._FeatureTable(ids=request.user_ids, table_name="user")

        features = list()
        for user_id in request.user_ids:
            features.append(user_dic[user_id])

        return np.array(features)

    def _ContentFeatures(
            self, request: PointwiseEstimatesRequest) -> np.ndarray:
        content_dic = self._FeatureTable(
            ids=request.content_ids, table_name="movie")

        features = list()
        for content_id in request.content_ids:
            features.append(content_dic[content_id])

        return np.array(features)

    def ComputePointwiseEstimates(
            self,
            request: PointwiseEstimatesRequest,
            context: grpc.ServicerContext) -> PointwiseEstimatesResponse:
        user_ids = self._UserIdsAsNumpy(request=request)
        user_features = self._UserFeatures(request=request)
        content_features = self._ContentFeatures(request=request)

        probs = self.model.predict([user_ids, user_features, content_features])

        return PointwiseEstimatesResponse(probs=probs.tolist())
