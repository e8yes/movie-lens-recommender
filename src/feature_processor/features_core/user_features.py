from pyspark.sql import DataFrame
from pyspark.sql.functions import array_contains, col, explode
from src.ingestion.database.reader import IngestionReaderInterface
from src.ingestion.database.reader_psql import ConfigurePostgresSparkSession
from src.ingestion.database.reader_psql import PostgresIngestionReader
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, get_json_object, expr, avg, count, broadcast
from pyspark.sql import functions as F
from pyspark.sql import types as T
from src.ingestion.database.reader import ReadContents
from src.ingestion.database.reader import ReadUsers
from src.ingestion.database.reader import ReadRatingFeedbacks
from pyspark.sql.functions import array_contains, col, explode,  mean, stddev, substring, split, stddev_pop, avg, broadcast, regexp_replace
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.linalg import SparseVector, DenseVector


def ComputeNormalizedAverageRating(
        user_rating_feebacks: DataFrame) -> DataFrame:
    """Computes the average rating each user gives. Then it applies the
    following transformation to the average ratings:
        normalized_avg_ratings =
            (avg_ratings[user_id] - mean(avg_ratings))/std(avg_ratings)

    Example input:
    ------------------------
    | user_id    | rating  |
    ------------------------
    |  1         |  3      |
    ------------------------
    |  1         |  5      |
    ------------------------
    |  2         |  3      |
    ------------------------
    |  3         |  2      |
    ------------------------
    |  3         |  2      |
    ------------------------

    Average ratings (intermediate result):
    ---------------------------
    | user_id    | avg_rating |
    ---------------------------
    |  1         |  4         |
    ---------------------------
    |  2         |  3         |
    ---------------------------
    |  3         |  2         |
    ---------------------------
    mean = 3, std = sqrt(2/3)

    Example output:
    -------------------
    | id | avg_rating |
    -------------------
    |  1 |  1.2247    |
    -------------------
    |  2 |  0         |
    -------------------
    |  3 | -1.2247    |
    -------------------

    Args:
        user_rating_feebacks (DataFrame): See the example input above.

    Returns:
        DataFrame: See the example output above.
    """
    user_rating = user_rating_feebacks.select('user_id', 'rating')
    user_rating = user_rating.groupBy('user_id').agg(
        avg("rating").alias("avg_rating_unscaled"))
    summary = user_rating.select([mean('avg_rating_unscaled').alias(
        'mu'), stddev('avg_rating_unscaled').alias('sigma')]).collect().pop()
    dft = user_rating.withColumn(
        'avg_rating', (user_rating['avg_rating_unscaled'] - summary.mu) / summary.
        sigma).select(
        col("user_id").alias("id"),
        'avg_rating')
    return dft


def ComputeNormalizedRatingFeedbackCount(
        user_rating_feebacks: DataFrame) -> DataFrame:
    """Computes the number of ratings each user gives. Then it applies the
    following transformation to the counts:
        normalized_count =
            (rating_count[user_id] - mean(rating_counts))/std(rating_counts)

    Example input:
    ------------------------
    | user_id    | rating  |
    ------------------------
    |  1         |  3      |
    ------------------------
    |  1         |  5      |
    ------------------------
    |  2         |  3      |
    ------------------------
    |  3         |  2      |
    ------------------------
    |  3         |  2      |
    ------------------------
    |  3         |  1      |
    ------------------------

    Rating count (intermediate result):
    -----------------------------
    | user_id    | rating_count |
    -----------------------------
    |  1         |  2           |
    -----------------------------
    |  2         |  1           |
    -----------------------------
    |  3         |  3           |
    -----------------------------
    mean = 2, std = sqrt(2/3)

    Example output:
    ---------------------
    | id | rating_count |
    ---------------------
    |  1 |  0           |
    ---------------------
    |  2 | -1.2247      |
    ---------------------
    |  3 |  1.2247      |
    ---------------------

    Args:
        user_rating_feebacks (DataFrame): See the example input above.

    Returns:
        DataFrame: See the example output above.
    """
    user_rating = user_rating_feebacks.select('user_id', 'rating')
    user_rating = user_rating.groupBy('user_id').agg(
        count("*").alias("count_unscaled"))
    summary = user_rating.select(
        [mean('count_unscaled').alias('mu'),
         stddev('count_unscaled').alias('sigma')]).collect().pop()
    rating_count_scaled = user_rating.withColumn(
        'rating_count', (user_rating['count_unscaled'] - summary.mu) / summary.
        sigma).select(
        col('user_id').alias('id'),
        'rating_count')
    return rating_count_scaled


def ComputeNormalizedTaggingFeedbackCount(
        user_tagging_feebacks: DataFrame) -> DataFrame:
    """Computes the number of tags each user gives. Then it applies the
    following transformation to the counts:
        normalized_count =
            (rating_count[user_id] - mean(rating_counts))/std(rating_counts)

    Example input:
    ------------------------------
    | user_id    | tag           |
    ------------------------------
    |  1         | "terrible     |
    ------------------------------
    |  1         | "horror"      |
    ------------------------------
    |  2         | "mesmerizing" |
    ------------------------------
    |  3         | "90s"         |
    ------------------------------
    |  3         | "nostalgia"   |
    ------------------------------
    |  3         | "classic"     |
    ------------------------------

    Rating count (intermediate result):
    ------------------------------
    | user_id    | tagging_count |
    ------------------------------
    |  1         |  2            |
    ------------------------------
    |  2         |  1            |
    ------------------------------
    |  3         |  3            |
    ------------------------------
    mean = 2, std = sqrt(2/3)

    Example output:
    ----------------------
    | id | tagging_count |
    ----------------------
    |  1 |  0            |
    ----------------------
    |  2 | -1.2247       |
    ----------------------
    |  3 |  1.2247       |
    ----------------------

    Args:
        user_tagging_feebacks (DataFrame): See the example input above.

    Returns:
        DataFrame: See the example output above.
    """
    user_tagging = user_tagging_feebacks.select('user_id', 'tag')
    user_tagging = user_tagging.groupBy('user_id').agg(
        count("*").alias("count_unscaled"))
    summary = user_tagging.select(
        [mean('count_unscaled').alias('mu'),
         stddev('count_unscaled').alias('sigma')]).collect().pop()
    tagging_count_scaled = user_tagging.withColumn(
        'tagging_count', (user_tagging['count_unscaled'] - summary.mu) / summary.
        sigma).select(
        col('user_id').alias('id'),
        'tagging_count')
    return tagging_count_scaled


def ComputeCoreUserFeatures(users: DataFrame,
                            user_rating_feebacks: DataFrame,
                            user_tagging_feedbacks: DataFrame) -> DataFrame:
    """Extracts core features from user rating and tagging feedbacks. See below
    for the list of core features.

    Args:
        users (DataFrame): The user dataframe with the schema as follows:
            root
                |-- id: long (nullable = false)
        user_rating_feebacks (DataFrame): The user rating feedback dataframe
            with the schema as follows:
            root
                |-- user_id: long (nullable = false)
                |-- content_id: long (nullable = false)
                |-- rated_at: timestamp (nullable = false)
                |-- rating: double (nullable = false)
        user_tagging_feedbacks (DataFrame): The user tagging feedback dataframe
            with the schema as follows:
            root
                |-- user_id: long (nullable = false)
                |-- content_id: long (nullable = false)
                |-- tag: string (nullable = false)
                |-- tagged_at: timestamp (nullable = false)

    Returns:
        DataFrame: A dataframe containing core user features, and the schema
        goes as below,
            root
                |-- id: long (nullable = false)
                |-- avg_rating: float (nullable = true)
                |-- rating_count: float (nullable = true)
                |-- tagging_count: float (nullable = true)
    """

    avg_rating = ComputeNormalizedAverageRating(user_rating_feebacks)
    rating_count = ComputeNormalizedRatingFeedbackCount(user_rating_feebacks)
    tagging_count = ComputeNormalizedTaggingFeedbackCount(
        user_tagging_feedbacks)
    res = users.join(
        avg_rating, ['id'],
        'left').join(
        rating_count, ['id'],
        'left').join(
        tagging_count, ['id'],
        'left')
    return res
