import numpy as np

__X_MIN = -100
__X_MAX = 100

__LEVEL = 0.5
__INTERVAL = (__X_MAX - __X_MIN)/10


def GenerateCloth(sample_count: int,
                  noise_level: float) -> np.ndarray:
    """_summary_

    Args:
        sample_count (int): _description_
        noise_level (float): _description_

    Returns:
        np.ndarray: _description_
    """
    x1 = np.random.uniform(low=__X_MIN, high=__X_MAX, size=sample_count)
    x2 = np.random.uniform(low=__X_MIN, high=__X_MAX, size=sample_count)
    noise = np.random.normal(loc=0.0, scale=noise_level, size=sample_count)

    z = x1*x1 + x2*x2*(__LEVEL + np.cos(1/__INTERVAL*x1)) + noise

    x1 = x1.reshape((sample_count, 1))
    x2 = x2.reshape((sample_count, 1))
    z = z.reshape((sample_count, 1))

    return np.concatenate([x1, x2, z], axis=1)
