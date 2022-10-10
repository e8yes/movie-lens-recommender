from typing import Tuple

from src.loader.reader_movie_lens import *
from src.loader.uploader import UploadDataFrame
from src.loader.uploader_user_profile import UserProfileUploader


def LoadMovieLensUserProfiles(data_set: MovieLensDataset,
                              ingestion_host: str) -> Tuple[int, int]:
    """Gathers user profile data and uploads them to the user ingestion
    service.

    Args:
        data_set (MovieLensDataset): The Movie Lens data set.
        ingestion_host (str): The host address which points to the ingestion
            server.

    Returns:
        Tuple[int, int]: #user profiles and #failed profiles.
    """
    unique_users = data_set.df_ratings.     \
        select([RATINGS_COL_USER_ID]).      \
        distinct().                         \
        cache()

    uploader = UserProfileUploader(host=ingestion_host,
                                   col_name_user_id=RATINGS_COL_USER_ID)
    failed_records = UploadDataFrame(data_frame=unique_users,
                                     uploader=uploader,
                                     num_retries=4)

    return unique_users.count(), failed_records.count()
