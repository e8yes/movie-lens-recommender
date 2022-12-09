from typing import Iterable
from pyspark.sql import Row

from src.feature_processor.features_common.generator import \
    GenerateMultiHotFeatures
from src.feature_processor.features_common.generator import \
    GenerateNormalFeatures

GENRE_COUNT = 20
LANGUAGE_COUNT = 30
DEPARTMENT_COUNT = 10


def GenerateFakeContentCoreFeaturesBatch(
        id_range: range, content_count: int) -> Iterable[Row]:
    """_summary_

    Args:
        id_range (range): _description_

    Returns:
        Iterable[Row]: Iterable of a row containing core content features
            where the schema goes as below,
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
    for id in id_range:
        yield Row(
            id=id,
            genres=GenerateMultiHotFeatures(category_count=GENRE_COUNT),
            languages=GenerateMultiHotFeatures(category_count=LANGUAGE_COUNT),
            avg_rating=id/content_count + GenerateNormalFeatures(),
            rating_count=id/content_count + GenerateNormalFeatures(),
            budget=id/content_count + GenerateNormalFeatures(),
            runtime=id/content_count + GenerateNormalFeatures(),
            release_year=id/content_count + GenerateNormalFeatures(),
            cast_composition=GenerateMultiHotFeatures(
                category_count=DEPARTMENT_COUNT),
            crew_composition=GenerateMultiHotFeatures(
                category_count=DEPARTMENT_COUNT),
            tmdb_avg_rating=id/content_count + GenerateNormalFeatures(),
            tmdb_vote_count=id/content_count + GenerateNormalFeatures())
