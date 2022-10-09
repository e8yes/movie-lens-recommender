from os import path
from pyspark.sql import DataFrame, SparkSession, types

# Data frame column names
GENOME_SCORES_COL_MOVIE_ID = "movieId"
GENOME_SCORES_COL_TAG_ID = "tagId"
GENOME_SCORES_COL_RELEVANCE = "relevance"

GENOME_TAG_COL_TAG_ID = "tagId"
GENOME_TAG_COL_TAG = "tag"

LINKS_COL_MOVIE_ID = "movieId"
LINKS_COL_IMDB_ID = "imdbId"
LINKS_COL_TMDB_ID = "tmdbId"

MOVIES_COL_MOVIE_ID = "movieId"
MOVIES_COL_TITLE = "title"
MOVIES_COL_GENRES = "genres"

RATINGS_COL_USER_ID = "userId"
RATINGS_COL_MOVIE_ID = "movieId"
RATINGS_COL_RATING = "rating"
RATINGS_COL_TIMESTAMP = "timestamp"

TAGS_COL_USER_ID = "userId"
TAGS_COL_MOVIE_ID = "movieId"
TAGS_COL_TAG = "tag"
TAGS_COL_TIMESTAMP = "tag"


class MovieLensDataset:
    """Represents the Movie Lens Data Set through loaded spark data frames.
    """

    def __init__(self, df_genome_scores: DataFrame,
                 df_genome_tags: DataFrame,
                 df_links: DataFrame,
                 df_movies: DataFrame,
                 df_ratings: DataFrame,
                 df_tags: DataFrame) -> None:
        self.df_genome_scores = df_genome_scores
        self.df_genome_tags = df_genome_tags
        self.df_links = df_links
        self.df_movies = df_movies
        self.df_ratings = df_ratings
        self.df_tags = df_tags


def ReadMovieLensDataSet(
        data_set_path: str, spark_session: SparkSession) -> MovieLensDataset:
    """Loads the Movie Lens data set from disk as spark data frames.

    Args:
        data_set_path (str): File path to the data sets folder.
        spark_session (SparkSession): Spark session.

    Returns:
        MovieLensDataset: The loaded Movie Lens data set.
    """
    path_movie_lens = path.join(data_set_path, "movie_lens")
    path_genome_scores = path.join(path_movie_lens, "genome-scores.csv")
    path_genome_tags = path.join(path_movie_lens, "genome-tags.csv")
    path_links = path.join(path_movie_lens, "links.csv")
    path_movies = path.join(path_movie_lens, "movies.csv")
    path_ratings = path.join(path_movie_lens, "ratings.csv")
    path_tags = path.join(path_movie_lens, "tags.csv")

    df_genome_scores = spark_session.read.option("header", "true").csv(
        path=path_genome_scores, schema=types.StructType(
            [types.StructField(GENOME_SCORES_COL_MOVIE_ID, types.IntegerType()),
             types.StructField(GENOME_SCORES_COL_TAG_ID, types.IntegerType()),
             types.StructField(GENOME_SCORES_COL_RELEVANCE, types.FloatType()),
             ]))
    df_genome_tags = spark_session.read.option("header", "true").csv(
        path=path_genome_tags, schema=types.StructType(
            [types.StructField(GENOME_TAG_COL_TAG_ID, types.IntegerType()),
             types.StructField(GENOME_TAG_COL_TAG, types.StringType()),
             ]))
    df_links = spark_session.read.option("header", "true").csv(
        path=path_links, schema=types.StructType(
            [types.StructField(LINKS_COL_MOVIE_ID, types.IntegerType()),
             types.StructField(LINKS_COL_IMDB_ID, types.IntegerType()),
             types.StructField(LINKS_COL_TMDB_ID, types.IntegerType()),
             ]))
    df_movies = spark_session.read.option("header", "true").csv(
        path=path_movies, schema=types.StructType(
            [types.StructField(MOVIES_COL_MOVIE_ID, types.IntegerType()),
             types.StructField(MOVIES_COL_TITLE, types.StringType()),
             types.StructField(MOVIES_COL_GENRES, types.StringType()),
             ]))
    df_ratings = spark_session.read.option("header", "true").csv(
        path=path_ratings, schema=types.StructType(
            [types.StructField(RATINGS_COL_USER_ID, types.IntegerType()),
             types.StructField(RATINGS_COL_MOVIE_ID, types.IntegerType()),
             types.StructField(RATINGS_COL_RATING, types.FloatType()),
             types.StructField(RATINGS_COL_TIMESTAMP, types.IntegerType()),
             ]))
    df_tags = spark_session.read.option("header", "true").csv(
        path=path_tags, schema=types.StructType(
            [types.StructField(TAGS_COL_USER_ID, types.IntegerType()),
             types.StructField(TAGS_COL_MOVIE_ID, types.IntegerType()),
             types.StructField(TAGS_COL_TAG, types.StringType()),
             types.StructField(TAGS_COL_TIMESTAMP, types.IntegerType()),
             ]))

    return MovieLensDataset(df_genome_scores=df_genome_scores,
                            df_genome_tags=df_genome_tags,
                            df_links=df_links,
                            df_movies=df_movies,
                            df_ratings=df_ratings,
                            df_tags=df_tags)
