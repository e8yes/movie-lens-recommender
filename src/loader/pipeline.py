import logging
from pyspark.sql import SparkSession

from src.loader.pipeline_content_profile import LoadMovieLensContentProfiles
from src.loader.pipeline_user_feedback import LoadMovieLensUserFeedbacks
from src.loader.pipeline_user_profile import LoadMovieLensUserProfiles
from src.loader.reader_movie_lens import *


def LoadMovieLensDataSet(data_set_path: str,
                         ingestion_host: str,
                         feedback_host: str):
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')

    spark_session = SparkSession.builder.       \
        appName("loader").                      \
        getOrCreate()

    logging.info("LoadMovieLensDataSet(): Start loading data records.")
    data_set = ReadMovieLensDataSet(
        data_set_path=data_set_path, spark_session=spark_session)

    num_users, failures = LoadMovieLensUserProfiles(
        data_set=data_set, ingestion_host=ingestion_host)
    logging.info(
        "LoadMovieLensDataSet(): processed {num_users} user profiles, failures {failures}. ".
        format(num_users=num_users, failures=failures))

    num_contents, failures = LoadMovieLensContentProfiles(
        data_set=data_set, ingestion_host=ingestion_host)
    logging.info(
        "LoadMovieLensDataSet(): processed {num_contents} content profiles, failures {failures}. ".
        format(num_contents=num_contents, failures=failures))

    num_feedbacks, failures = LoadMovieLensUserFeedbacks(
        data_set=data_set, feedback_host=feedback_host)
    logging.info(
        "LoadMovieLensDataSet(): processed {num_feedbacks} user feedbacks, failures {failures}. ".
        format(num_feedbacks=num_feedbacks, failures=failures))

    logging.info("LoadMovieLensDataSet(): loading completed.")
