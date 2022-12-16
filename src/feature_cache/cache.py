import numpy as np
import pickle
import redis
from typing import List
from typing import Tuple

USER_RECORD_PREFIX = "U"
CONTENT_RECORD_PREFIX = "C"


class FeatureCacheConfig:
    def __init__(self,
                 redis_host: str,
                 redis_port: int,
                 key_space: str) -> None:
        """_summary_

        Args:
            redis_host (str): _description_
            redis_port (int): _description_
            key_space (str): _description_
        """
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.key_space = key_space


class FeatureCache:
    """_summary_
    """

    def __init__(self,
                 config: FeatureCacheConfig) -> None:
        """_summary_

        Args:
            redis_host (str): _description_
            redis_port (int): _description_
            key_space(str): _description_
        """
        self.r = redis.Redis(
            host=config.redis_host,
            port=config.redis_port)
        self.key_space = config.key_space

    def _CacheFeatures(
            self,
            ids: List[int],
            inds: List[int],
            features: List[np.ndarray],
            key_prefix: str) -> None:
        pipe = self.r.pipeline()

        for i in range(len(ids)):
            key = self.key_space + key_prefix + str(ids[i])
            value = (inds[i], features[i])

            pipe.set(key, pickle.dumps(value))

        pipe.execute()

    def _FetchFeatures(
            self,
            ids: List[int],
            key_prefix: str) -> Tuple[List[int], List[np.ndarray]]:
        pipe = self.r.pipeline()

        unique_ids = set(ids)
        for id in unique_ids:
            key = self.key_space + key_prefix + str(id)
            pipe.get(key)

        values = pipe.execute()

        #
        fetched_values = dict()
        i = 0
        for id in unique_ids:
            fetched_values[id] = pickle.loads(values[i])
            i += 1

        #
        inds = list()
        features = list()
        for id in ids:
            ind, feature = fetched_values[id]

            inds.append(ind)
            features.append(feature)

        return inds, features

    def CacheContentFeatures(
            self,
            content_ids: List[int],
            content_inds: List[int],
            content_features: List[np.ndarray]) -> None:
        """_summary_

        Args:
            content_ids (List[int]): _description_
            content_inds (List[int]): _description_
            content_features (List[np.ndarray]): _description_
        """
        self._CacheFeatures(ids=content_ids,
                            inds=content_inds,
                            features=content_features,
                            key_prefix=CONTENT_RECORD_PREFIX)

    def CacheUserFeatures(
            self,
            user_ids: List[int],
            user_inds: List[int],
            user_features: List[np.ndarray]) -> None:
        """_summary_

        Args:
            user_ids (List[int]): _description_
            user_inds (List[int]): _description_
            user_features (List[np.ndarray]): _description_
        """
        self._CacheFeatures(ids=user_ids,
                            inds=user_inds,
                            features=user_features,
                            key_prefix=USER_RECORD_PREFIX)

    def FetchUsersFeatures(
            self,
            user_ids: List[int]) -> Tuple[List[int],
                                          List[np.ndarray]]:
        """_summary_

        Args:
            user_ids (List[int]): _description_

        Returns:
            Tuple[List[int], List[np.ndarray]]: _description_
        """
        return self._FetchFeatures(
            ids=user_ids, key_prefix=USER_RECORD_PREFIX)

    def FetchContentsFeatures(
            self,
            content_ids: List[int]) -> Tuple[List[int],
                                             List[np.ndarray]]:
        """_summary_

        Args:
            content_ids (List[int]): _description_

        Returns:
            Tuple[List[int], List[np.ndarray]]: _description_
        """
        return self._FetchFeatures(
            ids=content_ids, key_prefix=CONTENT_RECORD_PREFIX)
