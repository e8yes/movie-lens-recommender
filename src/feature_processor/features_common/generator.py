import numpy as np
from typing import List


def GenerateNormalFeatures() -> float:
    return np.random.normal(loc=0.0, scale=1.0)


def GenerateMultiHotFeatures(category_count: int) -> List[float]:
    features = np.random.randint(low=0, high=2, size=category_count)
    return features.astype(dtype=np.float32).tolist()


def GenerateEmbeddingFeatures(embedding_size: int) -> List[float]:
    features = np.random.normal(
        loc=0.0, scale=1.0, size=embedding_size)
    return features.tolist()
