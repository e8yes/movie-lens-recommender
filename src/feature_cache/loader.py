import numpy as np
from pyspark.sql import DataFrame
from pyspark.sql import Row
from typing import Iterable
from typing import List

from src.feature_cache.cache import FeatureCache
from src.feature_cache.cache import FeatureCacheConfig

_BATCH_SIZE = 200
_ID_FIELD = "id"
_INDEX_FIELD = "index"
_NONE_FEATURE_FIELDS = [_INDEX_FIELD, _ID_FIELD]


def _ReadFieldData(row: Row, field_name: str) -> List[float]:
    """Some of the feature fields are zero dimensional while the others are
    one dimensional. This function unifies the access by putting a zero
    dimensional scalar into an array with exactly 1 element.
    """
    field_data = row[field_name]
    if type(field_data) == list:
        return field_data

    assert type(field_data) == float
    return [field_data]


def _CacheBatch(
        rows: List[Row],
        feature_fields: List[str],
        feature_type: str,
        cache: FeatureCache) -> None:
    ids = list()
    inds = list()
    features = list()
    for row in rows:
        ids.append(row[_ID_FIELD])
        inds.append(row[_INDEX_FIELD])

        field_data = _ReadFieldData(row=row, field_name=feature_fields[0])
        all_feature = np.array(field_data)

        for i in range(1, len(feature_fields)):
            field_data = _ReadFieldData(row=row, field_name=feature_fields[i])
            this_feature = np.array(field_data)

            all_feature = np.concatenate([all_feature, this_feature])

        features.append(all_feature)

    if feature_type == "user":
        cache.CacheUserFeatures(
            user_ids=ids, user_inds=inds, user_features=features)
    elif feature_type == "content":
        cache.CacheContentFeatures(
            content_ids=ids, content_inds=inds, content_features=features)
    else:
        assert False


def _CachePartition(
        rows: Iterable[Row],
        feature_fields: List[str],
        feature_type: str,
        cache_config: FeatureCacheConfig) -> Iterable[int]:
    cache = FeatureCache(config=cache_config)

    batch = list()

    for row in rows:
        batch.append(row)
        if len(batch) < _BATCH_SIZE:
            continue

        _CacheBatch(
            rows=batch,
            feature_fields=feature_fields,
            feature_type=feature_type,
            cache=cache)

        yield len(batch)

        batch.clear()

    if len(batch) > 0:
        _CacheBatch(
            rows=batch,
            feature_fields=feature_fields,
            feature_type=feature_type,
            cache=cache)

        yield len(batch)


def _CacheFeatures(
        features: DataFrame,
        feature_type: str,
        cache_config: FeatureCacheConfig) -> int:
    # Captures all the feature field names in the data frame.
    feature_fields = list()
    for field in features.schema.fields:
        if field.name in _NONE_FEATURE_FIELDS:
            continue
        feature_fields.append(field.name)

    assert len(feature_fields) > 0

    # Caches the data frame in parallel.
    return features.                                        \
        rdd.                                                \
        mapPartitions(
            lambda rows: _CachePartition(rows,
                                         feature_fields,
                                         feature_type,
                                         cache_config)).    \
        sum()


def LoadUserFeaturesToCache(
        user_features: DataFrame, cache_config: FeatureCacheConfig) -> int:
    """Loads features in the specified data frame to the user feature cache.

    Args:
        user_features (DataFrame): The data frame to be loaded.
        cache_config (FeatureCacheConfig): Feature cache configuration.

    Returns:
        int: The total number of entries loaded.
    """
    return _CacheFeatures(
        features=user_features,
        feature_type="user",
        cache_config=cache_config)


def LoadContentFeaturesToCache(
        content_features: DataFrame, cache_config: FeatureCacheConfig) -> int:
    """Loads features in the specified data frame to the content feature cache.

    Args:
        content_features (DataFrame): The data frame to be loaded.
        cache_config (FeatureCacheConfig): Feature cache configuration.

    Returns:
        int: The total number of entries loaded.
    """
    return _CacheFeatures(
        features=content_features,
        feature_type="content",
        cache_config=cache_config)
