import numpy as np

__WIDTH = 5.0
__RADIUS = 10.0


def GenerateMorbiusStrip(sample_count: int,
                         noise_level: float) -> np.ndarray:
    """_summary_

    Args:
        sample_count (int): _description_
        noise_level (float): _description_

    Returns:
        np.ndarray: _description_
    """
    s = np.random.uniform(low=-__WIDTH, high=__WIDTH, size=sample_count)
    t = np.random.uniform(low=0, high=2*np.pi, size=sample_count)
    noise = np.random.normal(loc=0.0, scale=noise_level, size=sample_count)

    r = __RADIUS + s*np.cos(0.5*t)
    x1 = r*np.cos(t)
    x2 = r*np.sin(t)
    z = s*np.sin(0.5*t) + noise

    x1 = x1.reshape((sample_count, 1))
    x2 = x2.reshape((sample_count, 1))
    z = z.reshape((sample_count, 1))

    return np.concatenate([x1, x2, z], axis=1)
