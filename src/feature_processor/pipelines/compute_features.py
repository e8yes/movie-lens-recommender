from pyspark.sql import DataFrame

from src.feature_processor.features_core.content_features \
    import ComputeCoreContentFeatures
from src.feature_processor.features_core.user_features \
    import ComputeCoreUserFeatures
from src.feature_processor.features_profile.features \
    import CollectUserFeaturesFromProfile
from src.feature_processor.features_text.features \
    import ComputeContentTextFeatures
from src.ingestion.database.reader import IngestionReaderInterface
from src.ingestion.database.reader import ReadContents
from src.ingestion.database.reader import ReadRatingFeedbacks
from src.ingestion.database.reader import ReadTaggingFeedbacks
from src.ingestion.database.reader import ReadUsers


def ComputeUserFeatures(reader: IngestionReaderInterface) -> DataFrame:
    """A pipeline which builds user features for ranking modeling.

    Args:
        reader (IngestionReaderInterface): A reader object which allows the
            function to access the ingestion database.

    Returns:
        DataFrame: A dataframe containing all content features, and the schema
            goes as below,
            root
                |-- id: long (nullable = false)
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
    contents = ReadContents(reader=reader)
    rating_feedbacks = ReadRatingFeedbacks(reader=reader)

    core_features = ComputeCoreContentFeatures(
        contents=contents,
        user_rating_feebacks=rating_feedbacks)

    text_features = ComputeContentTextFeatures(contents=contents)

    all_features = core_features.\
        join(other=text_features,
             on=core_features.id == text_features.id,
             how="inner").\
        drop(text_features.id)

    return all_features


def ComputeContentFeatures(reader: IngestionReaderInterface) -> DataFrame:
    """A pipeline which builds content features for ranking modeling.

    Args:
        reader (IngestionReaderInterface): A reader object which allows the
            function to access the ingestion database.

    Returns:
        DataFrame: A dataframe containing all user features, and the schema
        goes as below,
            root
                |-- id: long (nullable = false)
                |-- avg_rating: float (nullable = true)
                |-- rating_count: float (nullable = true)
                |-- tagging_count: float (nullable = true)
                |-- profile: array (nullable = true)
                |    |-- element: float (containsNull = false)
    """
    users = ReadUsers(reader=reader)
    rating_feedbacks = ReadRatingFeedbacks(reader=reader)
    tagging_feedbacks = ReadTaggingFeedbacks(reader=reader)

    core_features = ComputeCoreUserFeatures(
        users=users,
        user_rating_feebacks=rating_feedbacks,
        user_tagging_feedbacks=tagging_feedbacks)

    profile_features = CollectUserFeaturesFromProfile(user_ids=users)
    all_features = core_features.                       \
        join(other=profile_features,
             on=core_features.id == profile_features.id,
             how="inner")

    return all_features
