import argparse
import numpy as np
from math import floor
from os import path
from pyspark.sql import DataFrame
from pyspark.sql import Row
from pyspark.sql import SparkSession
from typing import Iterable
from typing import List
from typing import Tuple


from src.feature_processor.features_common.schema import \
    CONTENT_CORE_FEATURE_SCHEMA
from src.feature_processor.features_common.schema import \
    CONTENT_TEXT_FEATURE_SCHEMA
from src.feature_processor.features_common.schema import \
    USER_CORE_FEATURE_SCHEMA
from src.feature_processor.features_common.schema import \
    USER_PROFILE_FEATURE_SCHEMA
from src.feature_processor.features_core.content_features_fake import \
    GenerateFakeContentCoreFeaturesBatch
from src.feature_processor.features_text.features_fake import \
    GenerateFakeContentTextFeaturesBatch
from src.feature_processor.features_core.user_features_fake import \
    GenerateUserCoreFeaturesBatch
from src.feature_processor.features_profile.features_fake import \
    GenerateFakeUserProfileFeaturesBatch
from src.feature_processor.pipelines.writer import \
    TFRECORDS_OUTPUT_FORMATTER_PATH
from src.feature_processor.pipelines.writer import WriteAsTfRecordDataSet
from src.feature_processor.pipelines.writer import WriteAsParquetDataSet

BATCH_COUNT = 50


def CreateSparkSession() -> SparkSession:
    return SparkSession.                                                \
        builder.                                                        \
        appName("Fake Features Generator").                             \
        config("spark.jars", TFRECORDS_OUTPUT_FORMATTER_PATH).           \
        getOrCreate()


def BatchRanges(total_count: int) -> List[range]:
    batches = list()

    batch_size = floor(total_count/BATCH_COUNT)
    current_starting_range = 1

    for i in range(BATCH_COUNT):
        if i == BATCH_COUNT - 1:
            batches.append(range(current_starting_range, total_count + 1))
        else:
            batches.append(range(current_starting_range,
                                 current_starting_range + batch_size))

        current_starting_range += batch_size

    return batches


def GenerateFakeUserProfiles(
        user_count: int, spark: SparkSession) -> DataFrame:
    batches = BatchRanges(total_count=user_count)

    core = spark.                               \
        sparkContext.                                   \
        parallelize(c=batches).                 \
        flatMap(GenerateUserCoreFeaturesBatch). \
        toDF(schema=USER_CORE_FEATURE_SCHEMA)

    profile = spark.                                    \
        sparkContext.                                   \
        parallelize(c=batches).                         \
        flatMap(GenerateFakeUserProfileFeaturesBatch).  \
        toDF(schema=USER_PROFILE_FEATURE_SCHEMA)

    return core.join(profile, ["id"])


def GenerateFakeContentProfiles(
        content_count: int, spark: SparkSession) -> DataFrame:
    batches = BatchRanges(total_count=content_count)

    core = spark.                                       \
        sparkContext.                                   \
        parallelize(c=batches).                         \
        flatMap(lambda r: GenerateFakeContentCoreFeaturesBatch(
            id_range=r, content_count=content_count)).  \
        toDF(schema=CONTENT_CORE_FEATURE_SCHEMA)

    text = spark.                                       \
        sparkContext.                                   \
        parallelize(c=batches).                         \
        flatMap(GenerateFakeContentTextFeaturesBatch).  \
        toDF(schema=CONTENT_TEXT_FEATURE_SCHEMA)

    return core.join(text, ["id"])


def GenerateFakeUserRatingBatch(
        user_id_range: range,
        user_count: int,
        content_count: int) -> Iterable[Row]:
    for user_id in user_id_range:
        high = floor(content_count*(0.2 + user_id/user_count))
        high = max(1, high)
        high = min(content_count, high)

        content_ids = np.random.randint(
            low=1, high=high + 1, size=30).tolist()
        content_ids = set(content_ids)
        content_ids = sorted(content_ids)

        content_ratings = np.random.uniform(
            low=0.0, high=1.0, size=len(content_ids)).tolist()
        content_ratings = sorted(content_ratings)

        i = 0
        for content_id in content_ids:
            yield Row(user_id=user_id,
                      content_id=content_id,
                      rating=content_ratings[i])
            i += 1


def GenerateFakeUserRatings(user_count: int,
                            content_count: int,
                            spark: SparkSession) -> DataFrame:
    batches = BatchRanges(total_count=user_count)
    return spark.\
        sparkContext.\
        parallelize(c=batches).\
        flatMap(
            lambda r: GenerateFakeUserRatingBatch(
                user_id_range=r,
                user_count=user_count,
                content_count=content_count)).\
        toDF()


def GenerateFakeUserRatingSets(
        user_count: int,
        content_count: int,
        spark: SparkSession) -> Tuple[DataFrame, DataFrame, DataFrame]:
    ratings = GenerateFakeUserRatings(user_count=user_count,
                                      content_count=content_count,
                                      spark=spark)

    training_set = ratings.where(ratings.user_id <= floor(user_count*0.8))
    valid_set = ratings.where((ratings.user_id > floor(user_count*0.8)) &
                              (ratings.user_id <= floor(user_count*0.9)))
    test_set = ratings.where(ratings.user_id > floor(user_count*0.9))

    return training_set, valid_set, test_set


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generates fake rating data sets with content and user "
                    "features to aid ranking model development.")
    parser.add_argument(
        "--user_count",
        type=int,
        help="The number of users to put into the data set.")
    parser.add_argument(
        "--content_count",
        type=int,
        help="The number of pieces of content to put into the data set.")
    parser.add_argument(
        "--output_path",
        type=str,
        help="Path where the rating training, validation and test sets as well"
        " as the user and content features are going to be written.")

    args = parser.parse_args()

    spark = CreateSparkSession()

    user_profiles = GenerateFakeUserProfiles(
        user_count=args.user_count, spark=spark)
    content_profiles = GenerateFakeContentProfiles(
        content_count=args.content_count, spark=spark)

    training_set, valid_set, test_set = GenerateFakeUserRatingSets(
        user_count=args.user_count,
        content_count=args.content_count,
        spark=spark)

    WriteAsParquetDataSet(
        df=user_profiles,
        output_path=path.join(args.output_path, "user_features"))
    WriteAsParquetDataSet(
        df=content_profiles,
        output_path=path.join(args.output_path, "content_features"))

    WriteAsParquetDataSet(
        df=training_set,
        output_path=path.join(args.output_path, "ratings", "parquet", "train"))
    WriteAsParquetDataSet(
        df=valid_set,
        output_path=path.join(
            args.output_path, "ratings", "parquet", "validation"))
    WriteAsParquetDataSet(
        df=test_set,
        output_path=path.join(args.output_path, "ratings", "parquet", "test"))

    WriteAsTfRecordDataSet(
        df=training_set, output_path=path.join(
            args.output_path, "ratings", "tfrecords", "train"))
    WriteAsTfRecordDataSet(
        df=valid_set,
        output_path=path.join(
            args.output_path, "ratings", "tfrecords", "validation"))
    WriteAsTfRecordDataSet(
        df=test_set,
        output_path=path.join(
            args.output_path, "ratings", "tfrecords", "test"))
