from typing import Tuple

from src.loader.reader_movie_lens import *
from src.loader.uploader import UploadDataFrame
from src.loader.uploader_user_feedback import UserFeedbackUploader


def LoadMovieLensUserFeedbacks(data_set: MovieLensDataset,
                               feedback_host: str) -> Tuple[int, int]:
    """Gathers user-content rating feedback data and uploads them to the
    feedback server.

    Args:
        data_set (MovieLensDataset): The Movie Lens data set.
        feedback_host (str): The host address which points to the user feedback
            server.

    Returns:
        Tuple[int, int]: #rating feedbacks and #failed feedbacks.
    """
    data_set.df_ratings.cache()

    uploader = UserFeedbackUploader(host=feedback_host,
                                    col_name_user_id=RATINGS_COL_USER_ID,
                                    col_name_content_id=RATINGS_COL_MOVIE_ID,
                                    col_name_timestamp=RATINGS_COL_TIMESTAMP,
                                    col_name_rating=RATINGS_COL_RATING)
    failed_records = UploadDataFrame(data_frame=data_set.df_ratings,
                                     uploader=uploader,
                                     num_retries=4)

    return data_set.df_ratings.count(), failed_records.count()
