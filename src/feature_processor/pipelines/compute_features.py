from pyspark.sql import DataFrame
from pyspark.sql import Row
from pyspark.sql import SparkSession

from src.feature_processor.features_common.schema import INDEX_FEATURE_SCHEMA
from src.feature_processor.features_core.content_features \
    import ComputeCoreContentFeatures
from src.feature_processor.features_core.user_features \
    import ComputeCoreUserFeatures
# from src.feature_processor.features_profile.features \
#     import CollectUserFeaturesFromProfile
from src.feature_processor.features_text.features \
    import ComputeContentTextFeatures
from src.feature_processor.imputation.mean import ImputateContentFeatures
from src.feature_processor.imputation.mean import ImputateUserFeatures
from src.ingestion.database.reader import IngestionReaderInterface
from src.ingestion.database.reader import ReadContents
from src.ingestion.database.reader import ReadRatingFeedbacks
from src.ingestion.database.reader import ReadTaggingFeedbacks
from src.ingestion.database.reader import ReadUsers


def _ComputeIndexFeature(df: DataFrame) -> DataFrame:
    id_and_inds = df.                                       \
        select("id").                                       \
        orderBy("id").                                      \
        rdd.                                                \
        zipWithIndex().                                     \
        map(lambda row_and_ind: Row(id=row_and_ind[0]["id"],
                                    index=row_and_ind[1])). \
        toDF(schema=INDEX_FEATURE_SCHEMA)

    return id_and_inds


def ComputeContentFeatures(reader: IngestionReaderInterface,
                           spark: SparkSession) -> DataFrame:
    """A pipeline which builds content features for ranking modeling.

    Args:
        reader (IngestionReaderInterface): A reader object which allows the
            function to access the ingestion database.
        spark (SparkSession): A vanilla spark session uses for creating data
            frames.

    Returns:
        DataFrame: A dataframe containing all content features, and the schema
            goes as below,
            root
                |-- id: long (nullable = false)
                |-- index: long (nullable = false)
                |-- genres: array (nullable = false)
                |    |-- element: float (containsNull = false)
                |-- languages: array (nullable = false)
                |    |-- element: float (containsNull = false)
                |-- avg_rating: float (nullable = true)
                |-- rating_count: float (nullable = true)
                |-- budget: float (nullable = true)
                |-- runtime: float (nullable = true)
                |-- release_year: float (nullable = true)
                |-- cast_composition: array (nullable = true)
                |    |-- element: float (containsNull = false)
                |-- crew_composition: array (nullable = true)
                |    |-- element: float (containsNull = false)
                |-- tmdb_avg_rating: float (nullable = true)
                |-- tmdb_vote_count: float (nullable = true)
                |-- summary: array (nullable = true)
                |    |-- element: float (containsNull = false)
                |-- tag: array (nullable = true)
                |    |-- element: float (containsNull = false)
                |-- keyword: array (nullable = true)
                |    |-- element: float (containsNull = false)
                |-- topic: array (nullable = true)
                |    |-- element: float (containsNull = false)
    """
    contents = ReadContents(reader=reader).checkpoint()
    rating_feedbacks = ReadRatingFeedbacks(reader=reader).checkpoint()

    index_feature = _ComputeIndexFeature(df=contents)

    core_features = ComputeCoreContentFeatures(
        contents=contents,
        user_rating_feebacks=rating_feedbacks).checkpoint()

    text_features = ComputeContentTextFeatures(
        contents=contents, spark=spark).checkpoint()

    all_features = index_feature.       \
        join(core_features, ["id"]).    \
        join(text_features, ["id"])

    return ImputateContentFeatures(content_features=all_features)


def ComputeUserFeatures(reader: IngestionReaderInterface) -> DataFrame:
    """A pipeline which builds user features for ranking modeling.

    Args:
        reader (IngestionReaderInterface): A reader object which allows the
            function to access the ingestion database.

    Returns:
        DataFrame: A dataframe containing all user features, and the schema
        goes as below,
            root
                |-- id: long (nullable = false)
                |-- index: long (nullable = false)
                |-- avg_rating: float (nullable = true)
                |-- rating_count: float (nullable = true)
                |-- tagging_count: float (nullable = true)
                |-- profile: array (nullable = true)
                |    |-- element: float (containsNull = false)
    """
    users = ReadUsers(reader=reader).checkpoint()
    rating_feedbacks = ReadRatingFeedbacks(reader=reader).checkpoint()
    tagging_feedbacks = ReadTaggingFeedbacks(reader=reader).checkpoint()

    index_feature = _ComputeIndexFeature(df=users)

    core_features = ComputeCoreUserFeatures(
        users=users,
        user_rating_feebacks=rating_feedbacks,
        user_tagging_feedbacks=tagging_feedbacks).checkpoint()

    # profile_features = CollectUserFeaturesFromProfile(user_ids=users)
    all_features = index_feature.       \
        join(core_features, ["id"])

    return ImputateUserFeatures(user_features=all_features)
