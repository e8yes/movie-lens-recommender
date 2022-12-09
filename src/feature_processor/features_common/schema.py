from pyspark.sql import types

#
CONTENT_CORE_FEATURE_SCHEMA = types.StructType(
    fields=[
        types.StructField(name="id", dataType=types.LongType()),
        types.StructField(name="genres",
                          dataType=types.ArrayType(
                              elementType=types.FloatType())),
        types.StructField(name="languages",
                          dataType=types.ArrayType(
                              elementType=types.FloatType())),
        types.StructField(name="avg_rating", dataType=types.FloatType()),
        types.StructField(name="rating_count", dataType=types.FloatType()),
        types.StructField(name="budget", dataType=types.FloatType()),
        types.StructField(name="runtime", dataType=types.FloatType()),
        types.StructField(name="release_year", dataType=types.FloatType()),
        types.StructField(name="cast_composition",
                          dataType=types.ArrayType(
                              elementType=types.FloatType())),
        types.StructField(name="crew_composition",
                          dataType=types.ArrayType(
                              elementType=types.FloatType())),
        types.StructField(name="tmdb_avg_rating", dataType=types.FloatType()),
        types.StructField(name="tmdb_vote_count", dataType=types.FloatType()),
    ])

#
CONTENT_TEXT_FEATURE_SCHEMA = types.StructType(
    fields=[
        types.StructField(name="id", dataType=types.LongType()),
        types.StructField(name="summary",
                          dataType=types.ArrayType(
                              elementType=types.FloatType())),
        types.StructField(name="tag",
                          dataType=types.ArrayType(
                              elementType=types.FloatType())),
        types.StructField(name="keyword",
                          dataType=types.ArrayType(
                              elementType=types.FloatType())),
        # types.StructField(name="topic",
        #                   dataType=types.ArrayType(
        #                       elementType=types.FloatType())),
    ])

#
USER_CORE_FEATURE_SCHEMA = types.StructType(
    fields=[
        types.StructField(name="id", dataType=types.LongType()),
        types.StructField(name="avg_rating", dataType=types.FloatType()),
        types.StructField(name="rating_count", dataType=types.FloatType()),
        types.StructField(name="tagging_count", dataType=types.FloatType()),
    ])

USER_PROFILE_FEATURE_SCHEMA = types.StructType(
    fields=[
        types.StructField(name="id", dataType=types.LongType()),
        types.StructField(name="profile",
                          dataType=types.ArrayType(
                              elementType=types.FloatType())),
    ])
