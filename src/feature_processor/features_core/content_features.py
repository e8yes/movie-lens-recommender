from pyspark.sql import DataFrame


def VectorizeGenres(content_genres: DataFrame) -> DataFrame:
    """Encodes a list of genre strings into a multi-hot vector (a list of
    floats).

    Example input:
    ---------------------------
    | id | genres             |
    ---------------------------
    |  1 | ["Action", "IMAX"] |
    ---------------------------

    Example output:
    ---------------------------
    | id | genres             |
    ---------------------------
    |  1 | [0,1,...,0,1,0]    |
    ---------------------------

    Args:
        content_genres (DataFrame): See the example input above.

    Returns:
        DataFrame: See the example output above.
    """
    content_genres.show()


def VectorizeLanguages(content_languages: DataFrame) -> DataFrame:
    """Encodes a list of language strings into a multi-hot vector (a list of
    floats).

    Example input:
    -------------------------------
    | id | languages              |
    -------------------------------
    |  1 | ["English", "Spanish"] |
    -------------------------------

    Example output:
    ---------------------------
    | id | languages          |
    ---------------------------
    |  1 | [0,1,...,0,1,0]    |
    ---------------------------

    Args:
        content_languages (DataFrame): See the example input above.

    Returns:
        DataFrame: See the example output above.
    """
    content_languages.show()


def ComputeNormalizedAverageRating(
        user_rating_feebacks: DataFrame) -> DataFrame:
    """Computes the average rating each piece of content receives. Then it
    applies the following transformation to the average ratings:
        normalized_avg_ratings =
            (avg_ratings[content_id] - mean(avg_ratings))/std(avg_ratings)

    Example input:
    ------------------------
    | content_id | rating  |
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
    | content_id | avg_rating |
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


def ComputeNormalizedRatingCount(
        user_rating_feebacks: DataFrame) -> DataFrame:
    """Computes the number of ratings each piece of content receives. Then it
    applies the following transformation to the counts:
        normalized_count =
            (rating_count[content_id] - mean(rating_counts))/std(rating_counts)

    Example input:
    ------------------------
    | content_id | rating  |
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
    | content_id | rating_count |
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


def NormalizeBudget(content_budget: DataFrame) -> DataFrame:
    """Transforms all budgets, so they distribute in a unit normal.

    Example input:
    -------------------
    | id | budget     |
    -------------------
    |  1 |  1,000,000 |
    -------------------
    |  3 |  3,000,000 |
    -------------------
    mean = 2,000,000, std = 1,000,000

    Example output:
    ---------------
    | id | budget |
    ---------------
    |  1 |  -1    |
    ---------------
    |  3 |   1    |
    ---------------

    Args:
        content_budget (DataFrame): See the example input above.

    Returns:
        DataFrame: See the example output above.
    """
    content_budget.show()


def NormalizeRuntime(content_runtime: DataFrame) -> DataFrame:
    """Transforms all runtimes, so they distribute in a unit normal.

    Example input:
    -------------------
    | id | runtime    |
    -------------------
    |  1 |  115       |
    -------------------
    |  3 |  75        |
    -------------------
    mean = 95, std = 1,000,000

    Example output:
    ----------------
    | id | runtime |
    ----------------
    |  1 |   1     |
    ----------------
    |  3 |  -1     |
    ----------------

    Args:
        content_runtime (DataFrame): See the example input above.

    Returns:
        DataFrame: See the example output above.
    """
    content_runtime.show()


def NormalizeReleaseYear(content_release_year: DataFrame) -> DataFrame:
    """Transform all the release years, so they distribute in a unit normal.

    Example input:
    ---------------------
    | id | release_year |
    ---------------------
    |  1 |  1980        |
    ---------------------
    |  2 |  2002        |
    ---------------------
    |  3 |  2012        |
    ---------------------
    mean = 1998, std = 178.67

    Example output:
    ---------------------
    | id | release_year |
    ---------------------
    |  1 |  -0.1        |
    ---------------------
    |  2 |   0.02       |
    ---------------------
    |  3 |   0.08       |
    ---------------------

    Args:
        content_release_year (DataFrame): See the example input above.

    Returns:
        DataFrame: See the example output above.
    """
    content_release_year.show()


def ComputeTeamComposition(
        content_credits: DataFrame) -> DataFrame:
    """Finds the composition of the content creation team. It first computes
    the absolute count for the number of people in each department, then it
    normalizes the count based on the mean and the standard deviation.

    Example input:
    --------------------------------------------------
    | id | tmdb_credits                              |
    --------------------------------------------------
    | 1  | '"cast": [                                |
    |    |  { "known_for_department": "Acting" },    |
    |    |  { "known_for_department": "Acting" }     |
    |    | ],                                        |
    |    | "crew": [                                 |
    |    |  { "known_for_department": "Directing" }, |
    |    |  { "known_for_department": "Writing" }    |
    |    | ]'                                        |
    --------------------------------------------------
    | 2  | '"cast": [                                |
    |    |  { "known_for_department": "Acting" }     |
    |    | ],                                        |
    |    | "crew": [                                 |
    |    |  { "known_for_department": "Directing" }, |
    |    |  { "known_for_department": "Sound" }      |
    |    | ]'                                        |
    --------------------------------------------------

    Intermediate result (count by department):
    -------------------------------------------
    | id | department          | cast | count |
    -------------------------------------------
    | 1  | "Acting"            | true | 2     |
    -------------------------------------------
    | 1  | "Directing"         | true | 0     |
    -------------------------------------------
    | 1  | "Writing"           | true | 0     |
    -------------------------------------------
    | 1  | "Production"        | true | 0     |
    -------------------------------------------
    | 1  | "Crew"              | true | 0     |
    -------------------------------------------
    | 1  | "Sound"             | true | 0     |
    -------------------------------------------
    | 1  | "Camera"            | true | 0     |
    -------------------------------------------
    | 1  | "Art"               | true | 0     |
    -------------------------------------------
    | 1  | "Costume & Make-Up" | true | 0     |
    -------------------------------------------
    | 1  | "Editing"           | true | 0     |
    -------------------------------------------
    | 1  | "Visual Effects"    | true | 0     |
    -------------------------------------------
    | 1  | "Lighting"          | true | 0     |
    -------------------------------------------
    | 1  | "Creator"           | true | 0     |
    -------------------------------------------

    ... ...


    Example output:
     ------------------------------------------------
    | id | cast_composition | crew_composition      |
    -------------------------------------------------
    | 1  | [1.0, 0.0, ...]  | [0.0, ..., -1.0, ...] |
    -------------------------------------------------
    | 2  | [-1.0, 0.0, ...] | [0.0, ..., 1.0, ...]  |
    -------------------------------------------------

    Args:
        content_credits (DataFrame): See the example input above.

    Returns:
        DataFrame: See the example output above.
    """
    content_credits.show()


def NormalizeTmdbVoteCount(content_tmdb_vote_count: DataFrame) -> DataFrame:
    """Transform all the TMDB vote counts, so they distribute in a unit normal.

    Example input:
    --------------------------
    | id | tmdb_vote_count   |
    --------------------------
    |  1 |  3000             |
    --------------------------
    |  2 |  2000             |
    --------------------------
    |  3 |  1000             |
    --------------------------
    mean = 2000, std = 816.5

    Example output:
    ------------------------
    | id | tmdb_vote_count |
    ------------------------
    |  1 |  1.2247         |
    ------------------------
    |  2 |  0              |
    ------------------------
    |  3 |  -1.2247        |
    ------------------------

    Args:
        content_tmdb_vote_count (DataFrame): See the example input above.

    Returns:
        DataFrame: See the example output above.
    """
    content_tmdb_vote_count.show()


def NormalizeTmdbAverageRating(
        content_tmdb_avg_rating: DataFrame) -> DataFrame:
    """Transform all the TMDB average ratings, so they distribute in a unit
    normal.

    Example input:
    --------------------------
    | id | tmdb_avg_rating   |
    --------------------------
    |  1 |  9.5              |
    --------------------------
    |  2 |  7                |
    --------------------------
    |  3 |  2                |
    --------------------------
    mean = 6.2, std = 3.5

    Example output:
    ------------------------
    | id | tmdb_avg_rating |
    ------------------------
    |  1 |   0.94          |
    ------------------------
    |  2 |   0.23          |
    ------------------------
    |  3 |  -1.2           |
    ------------------------

    Args:
        content_tmdb_avg_rating (DataFrame): See the example input above.

    Returns:
        DataFrame: See the example output above.
    """
    content_tmdb_avg_rating.show()


def ComputeCoreContentFeatures(contents: DataFrame,
                               user_rating_feebacks: DataFrame) -> DataFrame:
    """Extracts core features from the content dataframe as well as from the
    user rating feedbacks. See below for the list of core features.

    Args:
        contents (DataFrame): The content dataframe with the schema as follows,
            root
                |-- id: long (nullable = false)
                |-- title: string (nullable = true)
                |-- genres: array (nullable = true)
                |    |-- element: string (containsNull = false)
                |-- genome_scores: json (nullable = true)
                |-- tags: json (nullable = true)
                |-- imdb_id: integer (nullable = true)
                |-- tmdb_id: integer (nullable = true)
                |-- imdb_primary_info: json (nullable = true)
                |-- tmdb_primary_info: json (nullable = true)
                |-- tmdb_credits: json (nullable = true)
        user_rating_feebacks (DataFrame): The user rating feedback dataframe
            with the schema as follows:
            root
                |-- user_id: long (nullable = false)
                |-- content_id: long (nullable = false)
                |-- rated_at: timestamp (nullable = false)
                |-- rating: double (nullable = false)

    Returns:
        DataFrame: A dataframe containing core content features, and the schema
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
    """
    content_genres = VectorizeGenres(content_genres=contents.select(["id", "genres"]))
    contents.show()
    user_rating_feebacks.show()
