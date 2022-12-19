from typing import Tuple

from src.loader.reader_movie_lens import RATINGS_COL_USER_ID
from src.loader.reader_movie_lens import RATINGS_COL_MOVIE_ID
from src.loader.reader_movie_lens import RATINGS_COL_TIMESTAMP
from src.loader.reader_movie_lens import RATINGS_COL_RATING
from src.loader.reader_movie_lens import TAGS_COL_USER_ID
from src.loader.reader_movie_lens import TAGS_COL_MOVIE_ID
from src.loader.reader_movie_lens import TAGS_COL_TIMESTAMP
from src.loader.reader_movie_lens import TAGS_COL_TAG
from src.loader.reader_movie_lens import MovieLensDataset
from src.loader.uploader import UploadDataFrame
from src.loader.uploader_user_feedback import UserRatingUploader
from src.loader.uploader_user_feedback import UserTaggingUploader


def LoadMovieLensUserFeedbacks(data_set: MovieLensDataset,
                               feedback_host: str) -> Tuple[int, int]:
    """Gathers user-content rating and tagging feedback data and uploads them
    to the feedback server.

    Args:
        data_set (MovieLensDataset): The Movie Lens data set.
        feedback_host (str): The host address which points to the user feedback
            server.

    Returns:
        Tuple[int, int]: #total feedbacks and #failed feedbacks.
    """
    tagging_uploader = UserTaggingUploader(
        host=feedback_host,
        col_name_user_id=TAGS_COL_USER_ID,
        col_name_content_id=TAGS_COL_MOVIE_ID,
        col_name_timestamp=TAGS_COL_TIMESTAMP,
        col_name_tag=TAGS_COL_TAG)
    tagging_failures = UploadDataFrame(data_frame=data_set.df_tags,
                                       uploader=tagging_uploader,
                                       num_retries=4)

    scaled_ratings = data_set.                                          \
        df_ratings.                                                     \
        withColumn(colName="scaled_rating",
                   col=data_set.df_ratings[RATINGS_COL_RATING]/5.0).    \
        drop(RATINGS_COL_RATING)

    rating_uploader = UserRatingUploader(
        host=feedback_host,
        col_name_user_id=RATINGS_COL_USER_ID,
        col_name_content_id=RATINGS_COL_MOVIE_ID,
        col_name_timestamp=RATINGS_COL_TIMESTAMP,
        col_name_rating="scaled_rating")
    rating_failures = UploadDataFrame(data_frame=scaled_ratings,
                                      uploader=rating_uploader,
                                      num_retries=4)

    return data_set.df_tags.count() + data_set.df_ratings.count(),      \
        tagging_failures.count() + rating_failures.count()
