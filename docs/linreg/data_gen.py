from math import ceil
import sys
import os
import numpy as np


# Latent variables.
W = 1/3
B = 11
NOISE_STDDEV = 10000

# Sizes.
SAMPLE_SIZE = 1000000
X_MIN = -100000
X_MAX = 100000


def GenerateData(output_path: str):
    noises = np.random.normal(loc=0.0, scale=NOISE_STDDEV, size=SAMPLE_SIZE)
    xs = np.random.uniform(low=X_MIN, high=X_MAX, size=SAMPLE_SIZE)
    ys = W*xs + B + noises

    xs = xs.reshape((SAMPLE_SIZE, 1))
    ys = ys.reshape((SAMPLE_SIZE, 1))

    data_set = np.concatenate([xs, ys], axis=1)

    training_set_end = ceil(SAMPLE_SIZE*0.8)
    validation_set_end = ceil(SAMPLE_SIZE*0.9)
    test_set_end = SAMPLE_SIZE

    training_set = data_set[0:training_set_end, :]
    validation_set = data_set[training_set_end:validation_set_end, :]
    test_set = data_set[validation_set_end:test_set_end, :]

    np.savetxt(
        os.path.join(output_path, "training.csv"),
        training_set, delimiter=",")
    np.savetxt(
        os.path.join(output_path, "validation.csv"),
        validation_set, delimiter=",")
    np.savetxt(
        os.path.join(output_path, "test.csv"),
        test_set, delimiter=",")


if __name__ == "__main__":
    GenerateData(output_path=sys.argv[1])
