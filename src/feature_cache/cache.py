import numpy as np
import pickle
import redis
from typing import List
from typing import Tuple

USER_RECORD_PREFIX = "U"
CONTENT_RECORD_PREFIX = "C"


class FeatureCacheConfig:
    """In some cases where serializability is required, an actual FeatureCache
    can't be passed around. This allows a feature cache to be easily
    constructed on the spot where it's needed.
    """

    def __init__(self,
                 redis_host: str,
                 redis_port: int,
                 key_space: str) -> None:
        """Construct a feature cache configuration object.

        Args:
            redis_host (str): IP address to the Redis host.
            redis_port (int): Port of the Redis host.
            key_space (str): The keyspace where key-value pairs are going to
                be inserted into/read from.
        """
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.key_space = key_space


class FeatureCache:
    """A client object which stores/fetches feature data to/from a Redis store.
    Note that, this cache doesn't evict entries.
    """

    def __init__(self,
                 config: FeatureCacheConfig) -> None:
        """Constructs a feature cache client.

        Args:
            config (FeatureCacheConfig): See above.
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

        # De-duplicates the IDs to potentially save network traffic.
        unique_ids = set(ids)
        for id in unique_ids:
            key = self.key_space + key_prefix + str(id)
            pipe.get(key)

        values = pipe.execute()

        # Reads the results of the previous GET requests and keys each entry
        # by its ID.
        fetched_values = dict()
        i = 0
        for id in unique_ids:
            fetched_values[id] = pickle.loads(values[i])
            i += 1

        # Places the result in the order of the original ids array.
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
        """Writes/updates content features into the cache. The arguments must
        have the same length.

        Args:
            content_ids (List[int]): IDs/keys corresponding to the contents.
            content_inds (List[int]): Indices of the contents.
            content_features (List[np.ndarray]): Feature data.
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
        """Writes/updates user features into the cache. The arguments must
        have the same length

        Args:
            user_ids (List[int]): IDs/keys corresponding to the users.
            user_inds (List[int]): Indices of the users.
            user_features (List[np.ndarray]): Feature data.
        """
        self._CacheFeatures(ids=user_ids,
                            inds=user_inds,
                            features=user_features,
                            key_prefix=USER_RECORD_PREFIX)

    def FetchUsersFeatures(
            self,
            user_ids: List[int]) -> Tuple[List[int],
                                          List[np.ndarray]]:
        """Reads user features from the cache. User features referred to be
        the IDs must exist in the cache.

        Args:
            user_ids (List[int]): IDs/keys of the users whose feature will be
                fetched.

        Returns:
            Tuple[List[int], List[np.ndarray]]: Indices of the users and their
                corresponding feature data.
        """
        return self._FetchFeatures(
            ids=user_ids, key_prefix=USER_RECORD_PREFIX)

    def FetchContentsFeatures(
            self,
            content_ids: List[int]) -> Tuple[List[int],
                                             List[np.ndarray]]:
        """Reads content features from the cache. content features referred to
        be the IDs must exist in the cache.

        Args:
            content_ids (List[int]): Ds/keys of the contents whose feature
                will be fetched.

        Returns:
            Tuple[List[int], List[np.ndarray]]:  Indices of the contents and
                their corresponding feature data.
        """
        return self._FetchFeatures(
            ids=content_ids, key_prefix=CONTENT_RECORD_PREFIX)
