from pyspark import Row
from pyspark.sql import DataFrame, functions, types
from typing import Iterable, Tuple

from src.loader.reader_movie_lens import *
from src.loader.uploader import UploadDataFrame
from src.loader.uploader_content_profile import ContentProfileUploader


def __MovieLensContentGenresToList(content_df: DataFrame) -> DataFrame:
    genre_splitter = functions.split(
        str=content_df[MOVIES_COL_GENRES], pattern="\|")

    return content_df.select([content_df[MOVIES_COL_MOVIE_ID],
                              content_df[MOVIES_COL_TITLE],
                              genre_splitter]).                     \
        withColumnRenamed(existing="split({0}, \|, -1)".
                          format(MOVIES_COL_GENRES),
                          new=MOVIES_COL_GENRES)


def __MovieLensCollectGenomeScoresAsDict(
        entry: Tuple[int, Iterable[Row]]) -> Row:
    content_id, genome_score_kvs = entry

    genome_scores = dict()
    for row in genome_score_kvs:
        tag_id = row[GENOME_SCORES_COL_TAG_ID]
        relevance = row[GENOME_SCORES_COL_RELEVANCE]

        genome_scores[tag_id] = relevance

    ResultRow = Row(GENOME_SCORES_COL_MOVIE_ID, "genome_scores")
    return ResultRow(content_id, genome_scores)


def __MovieLensContentJoinGenomeScores(
        content_df: DataFrame, genome_score_df: DataFrame) -> DataFrame:
    # Collapes each piece of content's tag-relevance pairs into a sing row of
    # dictionary.
    genome_score_rdd = genome_score_df.rdd.                         \
        groupBy(lambda row: row[GENOME_SCORES_COL_MOVIE_ID]).       \
        map(__MovieLensCollectGenomeScoresAsDict)

    genome_score_schema = types.StructType([
        types.StructField(GENOME_SCORES_COL_MOVIE_ID, types.IntegerType()),
        types.StructField("genome_scores",
                          types.MapType(types.IntegerType(),
                                        types.FloatType()))
    ])

    genome_score_df = genome_score_rdd.toDF(schema=genome_score_schema)

    # Joins content with the genome scores.
    return content_df.                                              \
        join(other=genome_score_df,
             on=content_df[MOVIES_COL_MOVIE_ID] ==
             genome_score_df[GENOME_SCORES_COL_MOVIE_ID],
             how="leftouter").                                      \
        drop(genome_score_df[GENOME_SCORES_COL_MOVIE_ID])


def __MovieLensCollectTagsAsList(entry: Tuple[int, Iterable[Row]]) -> Row:
    content_id, tag_kvs = entry

    tags = dict()
    for row in tag_kvs:
        tag_str = row[TAGS_COL_TAG]
        timestamp = row[TAGS_COL_TIMESTAMP]

        tags[tag_str] = timestamp

    ResultRow = Row(GENOME_SCORES_COL_MOVIE_ID, TAGS_COL_TAG)
    return ResultRow(content_id, tags)


def __MovieLensContentJoinTags(content_df: DataFrame,
                               tag_df: DataFrame) -> DataFrame:
    # Collapes each piece of content's tag-timestamp pairs into a sing row of
    # dictionary.
    tag_rdd = tag_df.select([TAGS_COL_MOVIE_ID,
                            TAGS_COL_TAG,
                            TAGS_COL_TIMESTAMP]).                   \
        rdd.                                                        \
        groupBy(lambda row: row[TAGS_COL_MOVIE_ID]).                \
        map(__MovieLensCollectTagsAsList)

    tag_schema = types.StructType([
        types.StructField(TAGS_COL_MOVIE_ID, types.IntegerType()),
        types.StructField(TAGS_COL_TAG,
                          types.MapType(types.StringType(),
                                        types.IntegerType()))
    ])

    tag_df = tag_rdd.toDF(schema=tag_schema)

    # Joins content with the tags.
    return content_df.join(other=tag_df,
                           on=content_df[MOVIES_COL_MOVIE_ID] ==
                           tag_df[TAGS_COL_MOVIE_ID],
                           how="leftouter").                        \
        drop(tag_df[TAGS_COL_MOVIE_ID])


def __MovieLensContentJoinExternalLink(content_df: DataFrame,
                                       link_df: DataFrame) -> DataFrame:
    return content_df.                                              \
        join(
            other=link_df, on=content_df[MOVIES_COL_MOVIE_ID] ==
            link_df[LINKS_COL_MOVIE_ID],
            how="leftouter").                                       \
        drop(link_df[LINKS_COL_MOVIE_ID])


def LoadMovieLensContentProfiles(data_set: MovieLensDataset,
                                 ingestion_host: str) -> Tuple[int, int]:
    """Gathers content related data and uploads them to the content profile
    ingestion service.

    Args:
        data_set (MovieLensDataset): The Movie Lens data set.
        ingestion_host (str): The host address which points to the ingestion
            server.

    Returns:
        Tuple[int, int]: #content profiles and #failed profiles.
    """
    contents = __MovieLensContentGenresToList(content_df=data_set.df_movies)
    contents = __MovieLensContentJoinGenomeScores(
        content_df=contents, genome_score_df=data_set.df_genome_scores)
    contents = __MovieLensContentJoinTags(content_df=contents,
                                          tag_df=data_set.df_tags)
    contents = __MovieLensContentJoinExternalLink(content_df=contents,
                                                  link_df=data_set.df_links)
    contents.cache()

    uploader = ContentProfileUploader(host=ingestion_host,
                                      col_name_content_id=MOVIES_COL_MOVIE_ID,
                                      col_name_title=MOVIES_COL_TITLE,
                                      col_name_genres=MOVIES_COL_GENRES,
                                      col_name_genome_scores="genome_scores",
                                      col_name_tags=TAGS_COL_TAG,
                                      col_name_imdb_id=LINKS_COL_IMDB_ID,
                                      col_name_tmdb_id=LINKS_COL_TMDB_ID)
    failed_records = UploadDataFrame(data_frame=contents,
                                     uploader=uploader,
                                     num_retries=4)

    return contents.count(), failed_records.count()
