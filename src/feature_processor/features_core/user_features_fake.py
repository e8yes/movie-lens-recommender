from pyspark.sql import Row

from src.feature_processor.features_common.generator import \
    GenerateNormalFeatures


def GenerateUserCoreFeaturesBatch(id_ranges: range) -> Row:
    """_summary_

    Args:
        id_range (range): _description_

    Returns:
        DataFrame: A dataframe containing core user features, and the schema
        goes as below,
            root
                |-- id: long (nullable = false)
                |-- avg_rating: float (nullable = true)
                |-- rating_count: float (nullable = true)
                |-- tagging_count: float (nullable = true)
    """
    for id in id_ranges:
        yield Row(
            id=id,
            avg_rating=GenerateNormalFeatures(),
            rating_count=GenerateNormalFeatures(),
            tagging_count=GenerateNormalFeatures())
