from pyspark.sql import SparkSession

from src.feature_cache.cache import FeatureCacheConfig
from src.feature_cache.loader import LoadContentFeaturesToCache
from src.feature_cache.loader import LoadUserFeaturesToCache


def CreateSparkSession() -> SparkSession:
    """Creates a spark session suitable for the two following functions.
    """
    return SparkSession.            \
        builder.                    \
        appName("Feature Loader").  \
        getOrCreate()


def LoadContentFeatures(
        path: str,
        spark: SparkSession,
        feature_cache_config: FeatureCacheConfig) -> int:
    """Reads the content data frame and loads it into the feature cache.

    Args:
        path (str): Path to the content data frame.
        spark (SparkSession): A Spark session returned from
            CreateSparkSession().
        feature_cache_config (FeatureCacheConfig): Configuration of the
            feature cache.

    Returns:
        int: The number of content records loaded.
    """
    content_features = spark.read.parquet(path)
    return LoadContentFeaturesToCache(
        content_features=content_features, cache_config=feature_cache_config)


def LoadUserFeatures(
        path: str,
        spark: SparkSession,
        feature_cache_config: FeatureCacheConfig) -> int:
    """Reads the user data frame and loads it into the feature cache.

    Args:
        path (str): Path to the user data frame.
        spark (SparkSession): A Spark session returned from
            CreateSparkSession().
        feature_cache_config (FeatureCacheConfig): Configuration of the
            feature cache.

    Returns:
        int: The number of user records loaded.
    """
    user_features = spark.read.parquet(path)
    return LoadUserFeaturesToCache(
        user_features=user_features, cache_config=feature_cache_config)
