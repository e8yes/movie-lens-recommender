from pyspark.sql import Row
from typing import Iterable

from src.feature_processor.features_common.generator import \
    GenerateEmbeddingFeatures

USER_EMBEDDING_SIZE = 20


def GenerateFakeUserProfileFeaturesBatch(id_range: range) -> Iterable[Row]:
    for id in id_range:
        yield Row(
            id=id,
            profile=GenerateEmbeddingFeatures(
                embedding_size=USER_EMBEDDING_SIZE))
