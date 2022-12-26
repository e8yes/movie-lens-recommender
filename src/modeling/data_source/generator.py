import tensorflow as tf
from glob import glob
from os import path
from typing import Tuple

from src.feature_cache.cache import FeatureCache
from src.feature_cache.cache import FeatureCacheConfig

BATCH_SIZE = 300


def _DecodeFn(record_bytes: str) -> tf.Tensor:
    ratings = tf.io.parse_single_example(
        # Data
        record_bytes,
        # Schema
        {
            "user_id": tf.io.FixedLenFeature((), dtype=tf.int64),
            "content_id": tf.io.FixedLenFeature((), dtype=tf.int64),
            "rating": tf.io.FixedLenFeature((), dtype=tf.float32),
        }
    )

    return (ratings["user_id"], ratings["content_id"]), ratings["rating"]


def _FetchFeatures(
        x: Tuple[tf.Tensor, tf.Tensor],
        y: tf.Tensor,
        feature_cache: FeatureCache) -> tf.Tensor:
    def FetchUserFeatures(
            user_ids: tf.Tensor) -> Tuple[tf.Tensor, tf.Tensor]:
        user_inds, user_features = feature_cache.FetchUsersFeatures(
            user_ids=user_ids.numpy().tolist())
        return (
            tf.convert_to_tensor(user_inds, dtype=tf.int64),
            tf.convert_to_tensor(user_features, dtype=tf.float32)
        )

    def FetchContentFeatures(
            content_ids: tf.Tensor) -> Tuple[tf.Tensor, tf.Tensor]:
        content_inds, content_features = feature_cache.FetchContentsFeatures(
            content_ids=content_ids.numpy().tolist())
        return (
            tf.convert_to_tensor(content_inds, dtype=tf.int64),
            tf.convert_to_tensor(content_features, dtype=tf.float32)
        )

    user_ids, content_ids = x

    user_idx, user_features = tf.py_function(
        func=FetchUserFeatures,
        inp=[user_ids],
        Tout=(tf.int64, tf.float32))

    content_idx, content_features = tf.py_function(
        func=FetchContentFeatures,
        inp=[content_ids],
        Tout=(tf.int64, tf.float32))

    return (user_idx, user_features, content_idx, content_features), y


def GenerateUserContentPairs(
        rating_data_set_path: str,
        partition: str,
        feature_cache_config: FeatureCacheConfig) -> tf.data.Dataset:
    """Creates a data set iterator which generates the mapping:
        (user_index, user_feature, content_index, content_feature) -> rating.

    Args:
        rating_data_set_path (str): Path to the rating data set where it's
            going to iterate.
        partition (str): The partition of the data set to read from.
        feature_cache_config (FeatureCacheConfig): Configuration of a fully
            loaded feature cache.

    Returns:
        tf.data.Dataset: A data set iterator.
    """
    pathname = path.join(rating_data_set_path, partition, "part-r-*")

    data_set = tf.data.TFRecordDataset(
        filenames=glob(pathname=pathname),
        compression_type="GZIP")

    pairs_and_target = data_set.map(_DecodeFn)
    batches = pairs_and_target.batch(batch_size=BATCH_SIZE)
    feature_cache = FeatureCache(config=feature_cache_config)
    features = batches.map(lambda x, y: _FetchFeatures(x, y, feature_cache))

    return features
