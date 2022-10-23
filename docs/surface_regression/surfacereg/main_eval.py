import argparse
import json
from math import sqrt
import os
import numpy as np
import tensorflow as tf
from typing import Dict
from surfacereg.utils.plot import PlotSurfaceSamples, PlotTargetDistribution

from surfacereg.utils.reader import ValidationSet
from surfacereg.utils.reader import TestSet


def __ToNumpy(data_set: tf.data.Dataset):
    batch_xs = list()
    batch_zs = list()
    for batch_x, batch_z in data_set.batch(10000):
        batch_xs.append(batch_x)
        batch_zs.append(batch_z)

    return np.concatenate(batch_xs, axis=0), \
        np.concatenate(batch_zs, axis=0)


def __Evaluate(model: tf.keras.models.Model,
               data_set_path: str) -> Dict[str, float]:
    valid_x, valid_z = __ToNumpy(ValidationSet(data_set_path=data_set_path))
    test_x, test_z = __ToNumpy(TestSet(data_set_path=data_set_path))

    test_z_pred = model.predict(x=test_x)
    PlotTargetDistribution(z_true=test_z, z_pred=test_z_pred)
    PlotSurfaceSamples(x=test_x, z_true=test_z, z_pred=test_z_pred)

    mse_valid = model.evaluate(x=valid_x, y=valid_z)
    mse_test = model.evaluate(x=test_x, y=test_z)

    return {
        "validation_mean_distance": sqrt(mse_valid),
        "test_mean_distance": sqrt(mse_test),
        # "validation_hist_distance": 10.0,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Evaluates a trained surface model.")
    parser.add_argument(
        "--model_path", type=str,
        help="Directory to which the trained model is saved.")
    parser.add_argument(
        "--data_set_path", type=str,
        help="Directory of the surface data set.")

    args = parser.parse_args()
    if args.model_path is None:
        print("model_path is required.")
        exit(-1)
    if args.data_set_path is None:
        print("data_set_path is required.")
        exit(-1)

    model = tf.keras.models.load_model(filepath=args.model_path)
    evaluation = __Evaluate(model=model, data_set_path=args.data_set_path)

    evaluation_save_path = os.path.join(args.model_path, "evaluation.json")
    with open(file=evaluation_save_path, mode="wt") as file:
        json.dump(evaluation, file)
