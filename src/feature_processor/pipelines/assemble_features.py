from pyspark.sql import DataFrame

from src.feature_processor.features_core.content_features import ComputeCoreContentFeatures
from src.feature_processor.features_core.user_features import ComputeCoreUserFeatures
from src.feature_processor.features_profile.features import CollectUserFeaturesFromProfile
from src.feature_processor.features_text.features import ComputeContentTextFeatures
from src.feature_processor.features_text.features import ComputeUserTextFeatures
from src.ingestion.database.reader import IngestionReader
from src.ingestion.database.reader import ReadContents
from src.ingestion.database.reader import ReadRatingFeedbacks
from src.ingestion.database.reader import ReadTaggingFeedbacks
from src.ingestion.database.reader import ReadUsers


def AssembleUserFeatures(reader: IngestionReader) -> DataFrame:
    """_summary_

    Args:
        reader (IngestionReader): _description_

    Returns:
        DataFrame: _description_
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


def AssembleContentFeatures(reader: IngestionReader) -> DataFrame:
    """_summary_

    Args:
        reader (IngestionReader): _description_

    Returns:
        DataFrame: _description_
    """
    rating_feedbacks = ReadRatingFeedbacks(reader=reader)
    tagging_feedbacks = ReadTaggingFeedbacks(reader=reader)

    core_features = ComputeCoreUserFeatures(
        user_rating_feebacks=rating_feedbacks,
        user_tagging_feedbacks=tagging_feedbacks)

    text_features = ComputeUserTextFeatures(
        user_tagging_feedbacks=tagging_feedbacks)

    users = ReadUsers(reader=reader)
    profile_features = CollectUserFeaturesFromProfile(user_ids=users)

    all_features = core_features.\
        join(other=text_features,
             on=core_features.id == text_features.id,
             how="inner").\
        drop(text_features.id)

    all_features = all_features.\
        join(other=profile_features,
             on=all_features.id == profile_features.id,
             how="inner")

    return all_features
