from pyspark.sql import Row
from typing import Iterable

from src.feature_processor.features_common.generator import \
    GenerateEmbeddingFeatures

SUMMARY_EMBEDDING_SIZE = 100
TAG_EMBEDDING_SIZE = 100
KEYWORD_EMBEDDING_SIZE = 100
TOPIC_COUNT = 1129


def GenerateFakeContentTextFeaturesBatch(id_range: range) -> Iterable[Row]:
    for id in id_range:
        yield Row(
            id=id,
            summary=GenerateEmbeddingFeatures(
                embedding_size=SUMMARY_EMBEDDING_SIZE),
            tag=GenerateEmbeddingFeatures(
                embedding_size=TAG_EMBEDDING_SIZE),
            keyword=GenerateEmbeddingFeatures(
                embedding_size=KEYWORD_EMBEDDING_SIZE))
