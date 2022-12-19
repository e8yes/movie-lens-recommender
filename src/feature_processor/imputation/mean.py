from pyspark.sql import DataFrame
from pyspark.sql import Row
from typing import List


def _SetColumnNullToZero(row: Row, col: str) -> float:
    return 0.0 if row[col] is None else row[col]


def _SetColumnNullToNeg(row: Row, col: str) -> float:
    return -1.5 if row[col] is None else row[col]


def _SetColumnNullToVectorAverage(
        row: Row, col: str, avg: List[float]) -> List[float]:
    return avg if row[col] is None else row[col]


def _SumVector(a: List[float], b: List[float]) -> List[float]:
    sum = list()

    for i in range(len(a)):
        sum.append(a[i] + b[i])

    return sum


def _ComputeVectorAvg(df: DataFrame, col: str) -> List[float]:
    vectors = df.                   \
        select(col).                \
        where(df[col].isNotNull())

    vector_sum = vectors.           \
        rdd.                        \
        map(lambda row: row[col]).  \
        reduce(_SumVector)
    vector_count = vectors.count()

    vector_avg = list()
    for component in vector_sum:
        vector_avg.append(component/vector_count)

    return vector_avg


def _ImputateContent(
        row: Row,
        languages_avg: List[float],
        cast_composition_avg: List[float],
        crew_composition_avg: List[float],
        summary_avg: List[float],
        tag_avg: List[float],
        keyword_avg: List[float]) -> Row:
    return Row(
        id=row["id"],
        index=row["index"],
        genres=row["genres"],
        languages=_SetColumnNullToVectorAverage(
            row, "languages", languages_avg),
        avg_rating=_SetColumnNullToZero(row, "avg_rating"),
        rating_count=_SetColumnNullToZero(row, "rating_count"),
        budget=_SetColumnNullToZero(row, "budget"),
        runtime=_SetColumnNullToZero(row, "runtime"),
        release_year=_SetColumnNullToZero(row, "release_year"),
        cast_composition=_SetColumnNullToVectorAverage(
            row, "cast_composition", cast_composition_avg),
        crew_composition=_SetColumnNullToVectorAverage(
            row, "crew_composition", crew_composition_avg),
        tmdb_avg_rating=_SetColumnNullToZero(row, "tmdb_avg_rating"),
        tmdb_vote_count=_SetColumnNullToZero(row, "tmdb_vote_count"),
        summary=_SetColumnNullToVectorAverage(
            row, "summary", summary_avg),
        tag=_SetColumnNullToVectorAverage(
            row, "tag", tag_avg),
        keyword=_SetColumnNullToVectorAverage(
            row, "keyword", keyword_avg),
    )


def _ImputateUser(row: Row) -> Row:
    return Row(
        id=row["id"],
        index=row["index"],
        avg_rating=_SetColumnNullToZero(row, "avg_rating"),
        rating_count=_SetColumnNullToZero(row, "rating_count"),
        tagging_count=_SetColumnNullToNeg(row, "tagging_count"),
    )


def ImputateContentFeatures(content_features: DataFrame) -> DataFrame:
    """_summary_

    Args:
        content_features (DataFrame): _description_

    Returns:
        DataFrame: _description_
    """
    languages_avg = _ComputeVectorAvg(df=content_features, col="languages")
    cast_composition_avg = _ComputeVectorAvg(
        df=content_features, col="cast_composition")
    crew_composition_avg = _ComputeVectorAvg(
        df=content_features, col="crew_composition")
    summary_avg = _ComputeVectorAvg(df=content_features, col="summary")
    tag_avg = _ComputeVectorAvg(df=content_features, col="tag")
    keyword_avg = _ComputeVectorAvg(df=content_features, col="keyword")

    return content_features.                            \
        rdd.                                            \
        map(lambda row: _ImputateContent(
            row=row,
            languages_avg=languages_avg,
            cast_composition_avg=cast_composition_avg,
            crew_composition_avg=crew_composition_avg,
            summary_avg=summary_avg,
            tag_avg=tag_avg,
            keyword_avg=keyword_avg)).                  \
        toDF()


def ImputateUserFeatures(user_features: DataFrame) -> DataFrame:
    """_summary_

    Args:
        user_features (DataFrame): _description_

    Returns:
        DataFrame: _description_
    """
    return user_features.                           \
        rdd.                                        \
        map(lambda row: _ImputateUser(row=row)).    \
        toDF()
