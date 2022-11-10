from pyspark import Row
from pyspark.sql import DataFrame, functions, types
from typing import Iterable, Tuple

from src.loader.reader_movie_lens import *
from src.loader.uploader import UploadDataFrame
from src.loader.uploader_content_profile import ContentProfileUploader

SCORED_GENOME_TAGS_COLUMN = "scored_genome_tags"


def _ConvertGenresToList(content_df: DataFrame) -> DataFrame:
    genre_splitter = functions.split(
        str=content_df[MOVIES_COL_GENRES], pattern="\|")

    return content_df.select([content_df[MOVIES_COL_MOVIE_ID],
                              content_df[MOVIES_COL_TITLE],
                              genre_splitter]).                     \
        withColumnRenamed(existing="split({0}, \|, -1)".
                          format(MOVIES_COL_GENRES),
                          new=MOVIES_COL_GENRES)


def _ScoredGenomeTagsAsDict(
        entry: Tuple[int, Iterable[Row]]) -> Row:
    content_id, scored_genome_tag_kvs = entry

    scored_genome_tags = dict()
    for row in scored_genome_tag_kvs:
        tag = row[GENOME_TAG_COL_TAG]
        relevance = row[GENOME_SCORES_COL_RELEVANCE]

        scored_genome_tags[tag] = relevance

    ResultRow = Row(GENOME_SCORES_COL_MOVIE_ID, SCORED_GENOME_TAGS_COLUMN)
    return ResultRow(content_id, scored_genome_tags)


def _JoinGenomeTags(
        content_df: DataFrame,
        genome_score_df: DataFrame,
        genome_tag_df: DataFrame) -> DataFrame:
    # Replaces genome tag ID with the actual genome tag string
    # (denormalization).
    scored_genome_tags = genome_score_df.                       \
        join(other=genome_tag_df,
             on=genome_score_df[GENOME_SCORES_COL_TAG_ID] ==
             genome_tag_df[GENOME_TAG_COL_TAG_ID]).             \
        drop(genome_score_df[GENOME_SCORES_COL_TAG_ID]).        \
        drop(genome_tag_df[GENOME_TAG_COL_TAG_ID])

    # Groups multiple rows of tag-relevance pairs for each piece of content
    # into a single dictionary containing all the pairs.
    scored_genome_tag_rdd = scored_genome_tags.rdd.             \
        groupBy(lambda row: row[GENOME_SCORES_COL_MOVIE_ID]).   \
        map(_ScoredGenomeTagsAsDict)

    genome_score_schema = types.StructType([
        types.StructField(GENOME_SCORES_COL_MOVIE_ID, types.IntegerType()),
        types.StructField(SCORED_GENOME_TAGS_COLUMN,
                          types.MapType(types.StringType(),
                                        types.FloatType()))
    ])

    genome_score_df = scored_genome_tag_rdd.toDF(schema=genome_score_schema)

    # Joins content with the genome scores.
    return content_df.                                              \
        join(other=genome_score_df,
             on=content_df[MOVIES_COL_MOVIE_ID] ==
             genome_score_df[GENOME_SCORES_COL_MOVIE_ID],
             how="leftouter").                                      \
        drop(genome_score_df[GENOME_SCORES_COL_MOVIE_ID])


def _UserTagsAsList(entry: Tuple[int, Iterable[Row]]) -> Row:
    content_id, tag_kvs = entry

    tags = dict()
    for row in tag_kvs:
        tag_str = row[TAGS_COL_TAG]
        timestamp = row[TAGS_COL_TIMESTAMP]

        tags[tag_str] = timestamp

    ResultRow = Row(GENOME_SCORES_COL_MOVIE_ID, TAGS_COL_TAG)
    return ResultRow(content_id, tags)


def _JoinUserTags(content_df: DataFrame,
                  tag_df: DataFrame) -> DataFrame:
    # Collapes each piece of content's tag-timestamp pairs into a sing row of
    # dictionary.
    tag_rdd = tag_df.select([TAGS_COL_MOVIE_ID,
                            TAGS_COL_TAG,
                            TAGS_COL_TIMESTAMP]).                   \
        rdd.                                                        \
        groupBy(lambda row: row[TAGS_COL_MOVIE_ID]).                \
        map(_UserTagsAsList)

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


def _JoinExternalLink(content_df: DataFrame,
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
    contents = _ConvertGenresToList(content_df=data_set.df_movies)
    contents = _JoinGenomeTags(content_df=contents,
                               genome_score_df=data_set.df_genome_scores,
                               genome_tag_df=data_set.df_genome_tags)
    contents = _JoinUserTags(content_df=contents, tag_df=data_set.df_tags)
    contents = _JoinExternalLink(content_df=contents, link_df=data_set.df_links)
    contents.cache()

    uploader = ContentProfileUploader(
        host=ingestion_host,
        col_name_content_id=MOVIES_COL_MOVIE_ID,
        col_name_title=MOVIES_COL_TITLE,
        col_name_genres=MOVIES_COL_GENRES,
        col_name_scored_genome_tags=SCORED_GENOME_TAGS_COLUMN,
        col_name_tags=TAGS_COL_TAG,
        col_name_imdb_id=LINKS_COL_IMDB_ID,
        col_name_tmdb_id=LINKS_COL_TMDB_ID)
    failed_records = UploadDataFrame(data_frame=contents,
                                     uploader=uploader,
                                     num_retries=4)

    return contents.count(), failed_records.count()
