from pyspark.sql import DataFrame


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
    user_rating_feebacks.show()


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
    user_rating_feebacks.show()


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
    user_tagging_feebacks.show()


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
    users.show()
    user_rating_feebacks.show()
    user_tagging_feedbacks.show()
