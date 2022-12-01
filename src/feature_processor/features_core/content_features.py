from pyspark.sql import DataFrame

from pyspark.sql.functions import array_contains, col, explode
from src.ingestion.database.reader import IngestionReaderInterface
from src.ingestion.database.reader_psql import ConfigurePostgresSparkSession
from src.ingestion.database.reader_psql import PostgresIngestionReader
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, get_json_object, expr, avg, count
from pyspark.sql import functions as F
from pyspark.sql import types as T
from src.ingestion.database.reader import ReadContents
from src.ingestion.database.reader import ReadUsers
from src.ingestion.database.reader import ReadRatingFeedbacks
from pyspark.sql.functions import array_contains, col, explode,  mean, stddev
from pyspark.ml.feature import StringIndexer, VectorAssembler,StandardScaler
from pyspark.ml.linalg import SparseVector, DenseVector
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
    df1 = content_genres.select('id','genres')
    # df1.first()['genres'] ##2 7 8
    genres_list = [x[0] for x in df1.select(explode("genres").alias("genres")).distinct().orderBy("genres").collect()]
    df_sep = df1.select("*" ,*[
        array_contains("genres", g).alias("g_{}".format(g)).cast("integer")
        for g in genres_list]
    ).drop('genres')
    selected_columns = [column for column in df_sep.columns if column.startswith("g_")] 
    assembler = VectorAssembler(inputCols=selected_columns, outputCol='sparse_genres')
    df_sep = assembler.transform(df_sep).select('id','sparse_genres')
    def sparse_to_array(v):
        v = DenseVector(v)
        new_array = list([float(x) for x in v])
        return new_array
    sparse_to_array_udf = F.udf(sparse_to_array, T.ArrayType(T.FloatType()))
    res = df_sep.withColumn('genres', sparse_to_array_udf('sparse_genres')).select('id','genres')
    res.show()

def GetSpokenLanguages(content: DataFrame) -> DataFrame:
    """Extract list of languages strings from the column 'tmdb_primary_info', which is json object, 
        under 'spoken_languages' key

     Example input:
    -------------------------------
    | id | tmdb_primary_info      |
    -------------------------------
    |  1 | '{"id": 399168, "adult": false, "title": "The Mathematician and the Devil", "video":
             false, "budget": 0, "genres": [{"id": 35, "name": "Comedy"}, {"id": 18, "name": "Drama"}, 
             {"id": 14, "name": "Fantasy"}], "status": "Released", "imdb_id": "tt3154916", "revenue": 0, "runtime": 21, "tagline": "", "homepage": "", 
             "overview": "A mathematician offers to sell his soul to the devil for a proof or disproof of Fermat\'s Last Theorem. Based on \\"The Devil and Simon Flagg\\" by Arthur Porges.", "popularity": 0.6, 
             "vote_count": 6, "poster_path": "/5JCaWtCySRPy2JbHwgUAmYJBM8b.jpg", "release_date": "1972-06-06", "vote_average": 8.3,
             "backdrop_path": null, "original_title": "Математик и чёрт", 
             "spoken_languages": [{"name": "Pусский", "iso_639_1": "ru", "english_name": "Russian"}], 
             "original_language": "ru", "production_companies": [{"id": 88367, "name": "Centrnauchfilm", 
             "logo_path": "/8BGGqyuaxijzhqzmrgdCINWbPhj.png", "origin_country": "SU"}],
            "production_countries": [{"name": "Soviet Union", "iso_3166_1": "SU"}], "belongs_to_collection": null}') |
    -------------------------------

        Example output:
    -------------------------------
    | id | languages              |
    -------------------------------
    |  1 | ["English", "Spanish"] |
    -------------------------------
    """
    tmdb = content.select(["id","tmdb_primary_info"])
    lan = tmdb.withColumn('languages',get_json_object('tmdb_primary_info', '$.spoken_languages'))
    newdf= lan.withColumn('spoken_languages_all', F.from_json(
            F.col('languages'),
            T.ArrayType(T.StructType([
                T.StructField('name', T.StringType()),
                T.StructField('iso_639_1', T.StringType()),
                T.StructField('english_name', T.StringType()),
            ]))
        ))
    newdf = newdf.select('id','spoken_languages_all')
    spoken_languages = newdf.withColumn('languages' , expr("transform(spoken_languages_all, x -> x['english_name'])"))
    return spoken_languages.select('id','languages')

def VectorizeLanguages(spoken_languages: DataFrame) -> DataFrame:
    """Encodes a list of language strings into a multi-hot vector (a list of
    floats).

    Example input: (take input from GetSpokenLanguages)
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
    df1 = spoken_languages.select('id','languages')
    languages_list = [x[0] for x in df1.select(explode("languages").alias("languages")).distinct().orderBy("languages").collect()]
    df_sep = df1.select("*" ,*[
        array_contains("languages", lan).alias("l_{}".format(lan)).cast("integer")
        for lan in languages_list]
    ).drop('languages')
    selected_columns = [column for column in df_sep.columns if column.startswith("l_")] 
    assembler = VectorAssembler(inputCols=selected_columns, outputCol='sparse_languages',handleInvalid="skip")
    df_sep1 = assembler.transform(df_sep).select('id','sparse_languages').na.fill(value = 0, subset=["sparse_languages"])
    def sparse_to_array(v):
        v = DenseVector(v)
        new_array = list([float(x) for x in v])
        return new_array
    sparse_to_array_udf = F.udf(sparse_to_array, T.ArrayType(T.FloatType()))
    res = df_sep1.withColumn('languages', sparse_to_array_udf('sparse_languages')).select('id','languages')
    return res
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
    
    content_rating = user_rating_feebacks.select('content_id', 'rating')
    content_rating = content_rating.groupBy('content_id').agg(avg("rating").alias("avg_rating_unscaled"))
    summary = content_rating.select([mean('avg_rating_unscaled').alias('mu'), stddev('avg_rating_unscaled').alias('sigma')]).collect().pop()
    dft = content_rating.withColumn('avg_rating', (content_rating['avg_rating_unscaled']-summary.mu)/summary.sigma)
    return dft
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
    
    content_rating = user_rating_feebacks.select('user_id', 'rating')
    content_rating = content_rating.groupBy('user_id').agg(count("*").alias("count_unscaled"))
    summary = content_rating.select([mean('count_unscaled').alias('mu'), stddev('count_unscaled').alias('sigma')]).collect().pop()
    dft = content_rating.withColumn('rating_count', (content_rating['count_unscaled']-summary.mu)/summary.sigma)
    return dft


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
