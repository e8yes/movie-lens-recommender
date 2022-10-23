from glob import glob
import os
import tensorflow as tf
from typing import Tuple


def DecodeFn(record_bytes: str) -> Tuple[tf.Tensor, tf.Tensor]:
    example = tf.io.parse_single_example(
        # Data
        record_bytes,

        # Schema
        {
            "X": tf.io.FixedLenFeature((2), dtype=tf.float32),
            "Z": tf.io.FixedLenFeature((1), dtype=tf.float32)
        }
    )

    return example["X"], example["Z"]


def __DataSet(pathname: str) -> tf.data.Dataset:
    data_set = tf.data.TFRecordDataset(
        filenames=glob(pathname=pathname),
        compression_type="GZIP")

    return data_set.map(DecodeFn)


def TrainingSet(data_set_path: str) -> tf.data.Dataset:
    pathname = os.path.join(data_set_path, "training", "part-r-*")
    return __DataSet(pathname=pathname)


def ValidationSet(data_set_path: str) -> tf.data.Dataset:
    pathname = os.path.join(data_set_path, "validation", "part-r-*")
    return __DataSet(pathname=pathname)


def TestSet(data_set_path: str) -> tf.data.Dataset:
    pathname = os.path.join(data_set_path, "test", "part-r-*")
    return __DataSet(pathname=pathname)
