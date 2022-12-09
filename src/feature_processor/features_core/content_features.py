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
import json


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
    df1 = content_genres.select('id', 'genres')
    # df1.first()['genres'] ##2 7 8
    genres_list = [
        x[0]
        for x in df1.select(explode("genres").alias("genres")).distinct().orderBy(
            "genres").collect()]
    df_sep = df1.select("*", *[
        array_contains("genres", g).alias("g_{}".format(g)).cast("integer")
        for g in genres_list]
    ).drop('genres')
    selected_columns = [column for column in df_sep.columns
                        if column.startswith("g_")]
    assembler = VectorAssembler(
        inputCols=selected_columns, outputCol='sparse_genres')
    df_sep = assembler.transform(df_sep).select('id', 'sparse_genres')

    def sparse_to_array(v):
        v = DenseVector(v)
        new_array = list([float(x) for x in v])
        return new_array
    sparse_to_array_udf = F.udf(sparse_to_array, T.ArrayType(T.FloatType()))
    res = df_sep.withColumn(
        'genres', sparse_to_array_udf('sparse_genres')).select(
        'id', 'genres')
    return res


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
    tmdb = content.select(["id", "tmdb_primary_info"])
    lan = tmdb.withColumn('languages', get_json_object(
        'tmdb_primary_info', '$.spoken_languages'))
    newdf = lan.withColumn('spoken_languages_all', F.from_json(
        F.col('languages'),
        T.ArrayType(T.StructType([
            T.StructField('name', T.StringType()),
            T.StructField('iso_639_1', T.StringType()),
            T.StructField('english_name', T.StringType()),
        ]))
    ))
    newdf = newdf.select('id', 'spoken_languages_all')
    spoken_languages = newdf.withColumn('languages', expr(
        "transform(spoken_languages_all, x -> x['english_name'])"))
    return spoken_languages.select('id', 'languages')


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
    df1 = spoken_languages.select('id', 'languages')
    languages_list = [x[0]
                      for x in df1.select(
                          explode("languages").alias("languages")).distinct().orderBy(
                          "languages").collect()]
    df_sep = df1.select("*", *
                        [array_contains("languages", lan).alias(
                            "l_{}".format(lan)).cast("integer")
                         for lan in languages_list]).drop('languages')
    selected_columns = [column for column in df_sep.columns
                        if column.startswith("l_")]
    assembler = VectorAssembler(
        inputCols=selected_columns, outputCol='sparse_languages',
        handleInvalid="skip")
    df_sep1 = assembler.transform(df_sep).select(
        'id', 'sparse_languages').na.fill(
        value=0, subset=["sparse_languages"])

    def sparse_to_array(v):
        v = DenseVector(v)
        new_array = list([float(x) for x in v])
        return new_array
    sparse_to_array_udf = F.udf(sparse_to_array, T.ArrayType(T.FloatType()))
    res = df_sep1.withColumn(
        'languages', sparse_to_array_udf('sparse_languages')).select(
        'id', 'languages')
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
    content_rating = content_rating.groupBy('content_id').agg(
        avg("rating").alias("avg_rating_unscaled"))
    summary = content_rating.select([mean('avg_rating_unscaled').alias(
        'mu'), stddev('avg_rating_unscaled').alias('sigma')]).collect().pop()
    dft = content_rating.withColumn(
        'avg_rating', (content_rating['avg_rating_unscaled'] - summary.mu) /
        summary.sigma).select(
        col("content_id").alias("id"),
        'avg_rating')
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

    content_rating = user_rating_feebacks.select('content_id', 'rating')
    content_rating = content_rating.groupBy(
        'content_id').agg(count("*").alias("count_unscaled"))
    summary = content_rating.select([mean('count_unscaled').alias(
        'mu'), stddev('count_unscaled').alias('sigma')]).collect().pop()
    rating_count_scaled = content_rating.withColumn(
        'rating_count', (content_rating['count_unscaled'] - summary.mu) / summary.
        sigma).select(
        col('content_id').alias('id'),
        'rating_count')
    return rating_count_scaled


def GetBuget(content: DataFrame) -> DataFrame:
    """
    Extract budget from tmdb json column
    """
    content_budget = content.withColumn('budget_unscaled', get_json_object(
        'tmdb_primary_info', '$.budget')).select('id', 'budget_unscaled')

    return content_budget  # some values are null


def NormalizeBudget(content_budget: DataFrame) -> DataFrame:
    """Transforms all budgets, so they distribute in a unit normal.

    Example input:
    -------------------
    | id | budget_unscaled |
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
    budget_non0 = content_budget.filter(
        content_budget['budget_unscaled'] > 0)  # null/0 budgets are left unprocessed
    summary = budget_non0.select([mean('budget_unscaled').alias(
        'mu'), stddev('budget_unscaled').alias('sigma')]).collect().pop()
    budget_scaled = budget_non0.withColumn(
        'budget', (budget_non0['budget_unscaled']-summary.mu)/summary.sigma).select('id', 'budget')
    res = content_budget.join(
        budget_scaled, ['id'],
        'leftouter').select(
        'id', 'budget')
    return res


def GetRuntime(content: DataFrame) -> DataFrame:
    """
    Extract runtime from tmdb json column
    """
    content_runtime = content.withColumn('runtime_unscaled', get_json_object(
        'tmdb_primary_info', '$.runtime')).select('id', 'runtime_unscaled')
    return content_runtime


def NormalizeRuntime(content_runtime: DataFrame) -> DataFrame:
    """Transforms all runtimes, so they distribute in a unit normal.

    Example input:
    -------------------
    | id | runtime_unscaled    |
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
    runtime_non0 = content_runtime.filter(
        content_runtime['runtime_unscaled'] > 0)  # null/0 runtime are left unprocessed
    summary = runtime_non0.select([mean('runtime_unscaled').alias(
        'mu'), stddev('runtime_unscaled').alias('sigma')]).collect().pop()
    runtime_scaled = runtime_non0.withColumn(
        'runtime', (runtime_non0['runtime_unscaled']-summary.mu)/summary.sigma).select('id', 'runtime')
    res = content_runtime.join(
        runtime_scaled, ['id'],
        'leftouter').select(
        'id', 'runtime')
    return res


def GetReleaseYear(content: DataFrame) -> DataFrame:
    """
    Extract release year from tmdb json column
    """
    tmdb = content.select(["id", "tmdb_primary_info"])
    content_release_year = tmdb.withColumn(
        'release_date', get_json_object(
            'tmdb_primary_info', '$.release_date')).select(
        'id', 'release_date')
    content_release_year = content_release_year.select(
        'id', substring('release_date', 1, 4).alias('release_year_str'))
    content_release_year = content_release_year.withColumn(
        "release_year_unscaled", content_release_year["release_year_str"].cast(
            T.IntegerType())).select(
        'id', 'release_year_unscaled')
    return content_release_year


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

    content_release_year_nonNull = content_release_year.filter(
        content_release_year['release_year_unscaled'].isNotNull())  # null/0 budgets are left unprocessed
    summary = content_release_year_nonNull.select(
        [mean('release_year_unscaled').alias('mu'),
         stddev('release_year_unscaled').alias('sigma')]).collect().pop()
    release_year_scaled = content_release_year_nonNull.withColumn(
        'release_year',
        (content_release_year_nonNull['release_year_unscaled'] - summary.mu) /
        summary.sigma).select(
        'id', 'release_year')
    res = content_release_year.join(
        release_year_scaled, ['id'],
        'leftouter').select(
        'id', 'release_year')
    return res


def ComputeCasts(content: DataFrame) -> DataFrame:
    '''
     It first computes
#     the absolute count for the number of people in each department under 'Casts', then it
#     normalizes the count based on the mean and the standard deviation.

    Intermidiate result:
|    id|   |Acting|Actors|Art|Camera|Costume & Make-Up|Creator|Crew|Directing|Editing|Lighting|Production|Sound|Visual Effects|Writing|
+------+---+------+------+---+------+-----------------+-------+----+---------+-------+--------+----------+-----+--------------+-------+
| 97216|  0|    54|     0|  0|     0|                0|      0|   1|        0|      0|       0|         0|    0|             0|      0|
| 71936|  0|    47|     0|  0|     0|                0|      0|   0|        0|      0|       0|         1|    0|             0|      0|
| 62526|  0|    22|     0|  0|     0|                0|      0|   0|        0|      0|       0|         0|    0|             0|      0|

#     Example output:
#      ------------------------------------------------
#     | id | cast_composition | crew_composition      |
#     -------------------------------------------------
#     | 1  | [1.0, 0.0, ...]  | [0.0, ..., -1.0, ...] |
#     -------------------------------------------------
#     | 2  | [-1.0, 0.0, ...] | [0.0, ..., 1.0, ...]  |
#     -------------------------------------------------
    '''
    casts = content.withColumn(
        'cast', get_json_object('tmdb_credits', '$.cast')).select(
        'id', 'cast')
    casts = casts.filter(
        casts['cast'].isNotNull()).withColumn(
        'departments', F.udf(
            lambda x: [i['known_for_department'] for i in json.loads(x)])
        ('cast')).select(
        'id', 'departments')
    casts2 = casts.withColumn(
        "department",
        split(regexp_replace(col("departments"), r"(^\[)|(\]$)|(')", ""), ", ")
    )
    casts2 = casts2.select('id', 'department')
    casts2 = casts2.selectExpr(
        "id", "explode(department) as department").groupby("id").pivot(
        'department').count().na.fill(0)
    # casts2:  Intermidiate result
    # Normalize each column
    selected_columns = [column for column in casts2.columns
                        if column != 'id' and column != '']
    stats = (casts2.groupBy().agg(
        *([stddev_pop(x).alias(x + '_stddev') for x in selected_columns] +
          [avg(x).alias(x + '_avg') for x in selected_columns])))
    df2 = casts2.join(broadcast(stats))
    exprs = ['id']+[((df2[x] - df2[x + '_avg']) / df2[x + '_stddev']).alias(x)
                    for x in selected_columns]
    df2 = df2.select(exprs)
    # combine multiple columns into one feature:
    assembler = VectorAssembler(
        inputCols=selected_columns, outputCol='sparse_casts',
        handleInvalid="skip")
    df_sep1 = assembler.transform(df2).select(
        'id', 'sparse_casts').na.fill(
        value=0, subset=["sparse_casts"])
    # convert sparse array to dense array

    def sparse_to_array(v):
        v = DenseVector(v)
        new_array = list([float(x) for x in v])
        return new_array
    sparse_to_array_udf = F.udf(sparse_to_array, T.ArrayType(T.FloatType()))
    res = df_sep1.withColumn(
        'cast_composition', sparse_to_array_udf('sparse_casts')).select(
        'id', 'cast_composition')
    return res


def ComputeCrews(content: DataFrame) -> DataFrame:
    """
         It first computes
#     the absolute count for the number of people in each department under 'crew', then it
#     normalizes the count based on the mean and the standard deviation.

    """
    crews = content.withColumn(
        'crew', get_json_object('tmdb_credits', '$.crew')).select(
        'id', 'crew')
    crews = crews.filter(crews['crew'].isNotNull()).withColumn('departments', F.udf(
        lambda x: [i['department'] for i in json.loads(x)])('crew')).select('id', 'departments')
    crews2 = crews.withColumn(
        "department",
        split(regexp_replace(col("departments"), r"(^\[)|(\]$)", ""), ", ")
    )
    crews2 = crews2.select('id', 'department')
    crews2 = crews2.selectExpr(
        "id", "explode(department) as department").groupby("id").pivot(
        'department').count().na.fill(0)
    selected_columns = [column for column in crews2.columns
                        if column != 'id' and column != '']
    stats = (crews2.groupBy().agg(
        *([stddev_pop(x).alias(x + '_stddev') for x in selected_columns] +
          [avg(x).alias(x + '_avg') for x in selected_columns])))
    df2 = crews2.join(broadcast(stats))
    exprs = ['id']+[((df2[x] - df2[x + '_avg']) / df2[x + '_stddev']).alias(x)
                    for x in selected_columns]
    df2 = df2.select(exprs)
    assembler = VectorAssembler(
        inputCols=selected_columns, outputCol='sparse_crews',
        handleInvalid="skip")
    df_sep1 = assembler.transform(df2).select(
        'id', 'sparse_crews').na.fill(
        value=0, subset=["sparse_crews"])
    # convert sparse array to dense array

    def sparse_to_array(v):
        v = DenseVector(v)
        new_array = list([float(x) for x in v])
        return new_array
    sparse_to_array_udf = F.udf(sparse_to_array, T.ArrayType(T.FloatType()))
    res = df_sep1.withColumn(
        'crew_composition', sparse_to_array_udf('sparse_crews')).select(
        'id', 'crew_composition')
    return res
# def ComputeTeamComposition(
#         content_credits: DataFrame) -> DataFrame:
#     """Finds the composition of the content creation team. It first computes
#     the absolute count for the number of people in each department, then it
#     normalizes the count based on the mean and the standard deviation.

#     Example input:
#     --------------------------------------------------
#     | id | tmdb_credits                              |
#     --------------------------------------------------
#     | 1  | '"cast": [                                |
#     |    |  { "known_for_department": "Acting" },    |
#     |    |  { "known_for_department": "Acting" }     |
#     |    | ],                                        |
#     |    | "crew": [                                 |
#     |    |  { "known_for_department": "Directing" }, |
#     |    |  { "known_for_department": "Writing" }    |
#     |    | ]'                                        |
#     --------------------------------------------------
#     | 2  | '"cast": [                                |
#     |    |  { "known_for_department": "Acting" }     |
#     |    | ],                                        |
#     |    | "crew": [                                 |
#     |    |  { "known_for_department": "Directing" }, |
#     |    |  { "known_for_department": "Sound" }      |
#     |    | ]'                                        |
#     --------------------------------------------------

#     Intermediate result (count by department):
#     -------------------------------------------
#     | id | department          | cast | count |
#     -------------------------------------------
#     | 1  | "Acting"            | true | 2     |
#     -------------------------------------------
#     | 1  | "Directing"         | true | 0     |
#     -------------------------------------------
#     | 1  | "Writing"           | true | 0     |
#     -------------------------------------------
#     | 1  | "Production"        | true | 0     |
#     -------------------------------------------
#     | 1  | "Crew"              | true | 0     |
#     -------------------------------------------
#     | 1  | "Sound"             | true | 0     |
#     -------------------------------------------
#     | 1  | "Camera"            | true | 0     |
#     -------------------------------------------
#     | 1  | "Art"               | true | 0     |
#     -------------------------------------------
#     | 1  | "Costume & Make-Up" | true | 0     |
#     -------------------------------------------
#     | 1  | "Editing"           | true | 0     |
#     -------------------------------------------
#     | 1  | "Visual Effects"    | true | 0     |
#     -------------------------------------------
#     | 1  | "Lighting"          | true | 0     |
#     -------------------------------------------
#     | 1  | "Creator"           | true | 0     |
#     -------------------------------------------

#     ... ...


#     Example output:
#      ------------------------------------------------
#     | id | cast_composition | crew_composition      |
#     -------------------------------------------------
#     | 1  | [1.0, 0.0, ...]  | [0.0, ..., -1.0, ...] |
#     -------------------------------------------------
#     | 2  | [-1.0, 0.0, ...] | [0.0, ..., 1.0, ...]  |
#     -------------------------------------------------

#     Args:
#         content_credits (DataFrame): See the example input above.

#     Returns:
#         DataFrame: See the example output above.
#     """
#     content_credits.show()

def GetVoteCount(content: DataFrame) -> DataFrame:
    '''
    Extract Tmdb Vote count from content tmdb json column.
    '''
    tmdb = content.select(["id", "tmdb_primary_info"])
    content_tmdb_vote_count = tmdb.withColumn(
        'vote_count_unscaled',
        get_json_object('tmdb_primary_info', '$.vote_count')).select(
        'id', 'vote_count_unscaled')
    return content_tmdb_vote_count


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

    content_tmdb_vote_count_nonNull = content_tmdb_vote_count.filter(
        content_tmdb_vote_count['vote_count_unscaled'].isNotNull())  # null vote count are left unprocessed
    summary = content_tmdb_vote_count_nonNull.select(
        [mean('vote_count_unscaled').alias('mu'),
         stddev('vote_count_unscaled').alias('sigma')]).collect().pop()
    vote_count_scaled = content_tmdb_vote_count_nonNull.withColumn(
        'tmdb_vote_count',
        (content_tmdb_vote_count_nonNull['vote_count_unscaled'] - summary.mu) /
        summary.sigma).select(
        'id', 'tmdb_vote_count')
    res = content_tmdb_vote_count.join(
        vote_count_scaled, ['id'],
        'leftouter').select(
        'id', 'tmdb_vote_count')
    return res


def GetTmdbAverageRating(content: DataFrame) -> DataFrame:
    """
    Extract tmdb average rating from content tmdb json volumn.
    """
    content_tmdb_avg_rating = content.withColumn(
        'tmdb_avg_rating_unscaled',
        get_json_object('tmdb_primary_info', '$.vote_average')).select(
        'id', 'tmdb_avg_rating_unscaled')
    return content_tmdb_avg_rating


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
    tmdb_avg_rating_nonNull = content_tmdb_avg_rating.filter(
        content_tmdb_avg_rating['tmdb_avg_rating_unscaled'].isNotNull())  # null avg rating are left unprocessed
    summary = tmdb_avg_rating_nonNull.select([mean('tmdb_avg_rating_unscaled').alias(
        'mu'), stddev('tmdb_avg_rating_unscaled').alias('sigma')]).collect().pop()
    tmdb_avg_rating_scaled = tmdb_avg_rating_nonNull.withColumn(
        'tmdb_avg_rating',
        (tmdb_avg_rating_nonNull['tmdb_avg_rating_unscaled'] - summary.mu) /
        summary.sigma).select(
        'id', 'tmdb_avg_rating')
    res = content_tmdb_avg_rating.join(
        tmdb_avg_rating_scaled, ['id'],
        'leftouter').select(
        'id', 'tmdb_avg_rating')
    return res


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

    genres = VectorizeGenres(contents)
    spoken_language = GetSpokenLanguages(contents)
    languages = VectorizeLanguages(spoken_language)

    avg_rating_nonNull = ComputeNormalizedAverageRating(user_rating_feebacks)
    avg_rating = contents.select('id').join(avg_rating_nonNull, ['id'], 'left')

    rating_count_nonNull = ComputeNormalizedRatingCount(user_rating_feebacks)
    rating_count = contents.select('id').join(
        rating_count_nonNull, ['id'], 'left')

    dollar_budget = GetBuget(contents)
    budget = NormalizeBudget(dollar_budget)
    unscaled_runtime = GetRuntime(contents)
    runtime = NormalizeRuntime(unscaled_runtime)
    release_year_unscaled = GetReleaseYear(contents)
    release_year = NormalizeReleaseYear(release_year_unscaled)
    cast_composition = ComputeCasts(contents)
    crew_composition = ComputeCrews(contents)
    vote_count_unscaled = GetVoteCount(contents)
    tmdb_vote_count = NormalizeTmdbVoteCount(vote_count_unscaled)
    tmdb_avg_rating = GetTmdbAverageRating(contents)
    tmdb_avg_rating = NormalizeTmdbAverageRating(tmdb_avg_rating)
    return contents.select('id').join(
        genres, ['id'],
        'outer').join(
        languages, ['id'],
        'outer').join(
        avg_rating, ['id'],
        'outer').join(
        rating_count, ['id'],
        'outer').join(
        budget, ['id'],
        'outer').join(
        runtime, ['id'],
        'outer').join(
        release_year, ['id'],
        'outer').join(
        cast_composition, ['id'],
        'outer').join(
        crew_composition, ['id'],
        'outer').join(
        tmdb_avg_rating, ['id'],
        'outer').join(
        tmdb_avg_rating, ['id'],
        'outer').join(
        tmdb_vote_count, ['id'],
        'outer')
