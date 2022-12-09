import pandas as pd
import numpy as np
from zipfile import ZipFile
import tensorflow as tf
from tensorflow import keras
from pathlib import Path
import matplotlib.pyplot as plt
from glob import glob
import os
import tensorflow as tf
from typing import Tuple
import cassandra_session


def __DataSet(pathname: str) -> tf.data.Dataset:                                          #read file in the GZ
    data_set = tf.data.TFRecordDataset(
        filenames=glob(pathname=pathname),
        compression_type="GZIP")

    return data_set.map(DecodeFn).batch(2).map(to_string_lazy_function)


def to_string_user(batch) -> tf.Tensor:                                               
    set_r = set()
    r = list()
    user_dic = dict()
    for id in batch.numpy():
        set_r.add(id[0])

    sql = sql_builder(set_r,'user')
    rows= cassandra_session.session.execute(sql)

    for x in rows:
        user_dic.update({x[0]: x[1]})

    for id in batch.numpy():
        r.append(user_dic[id[0]])

    return tf.convert_to_tensor(value=r, dtype=tf.float32)


def to_string_content(batch) -> tf.Tensor:
    set_r = set()
    r = list()
    movie_dic = dict()
    for id in batch.numpy():
        set_r.add(id[0])

    sql = sql_builder(set_r,'movie')
    rows= cassandra_session.session.execute(sql)

    for x in rows:
        movie_dic.update({x[0]: x[1]})

    for id in batch.numpy():
        r.append(movie_dic[id[0]])

    return tf.convert_to_tensor(value=r, dtype=tf.float32)


def to_string_lazy_function(input,rate) -> tf.Tensor:

    user_feature = tf.py_function(
        func=to_string_user,
        inp=[input[0]],
        Tout=tf.float32)    

    content_feature = tf.py_function(
        func=to_string_content,
        inp=[input[1]],
        Tout=tf.float32)

    return (input[0],user_feature,content_feature,input[1]),rate


def sql_builder(set,table):
    sql = 'SELECT * FROM ' + table + ' where id in ('
    for x in set:
        sql = sql + str(x) + ','
    sql = sql[:-1] +');'
    return sql



def DecodeFn(record_bytes: str):
    ratings = tf.io.parse_single_example(
        # Data
        record_bytes,
        # Schema
        {
            'content_id': tf.io.FixedLenFeature((1), dtype=tf.int64),
            'rating': tf.io.FixedLenFeature((1), dtype=tf.float32),
            'user_id':tf.io.FixedLenFeature((1), dtype=tf.int64)
        }
    )

    return (ratings['user_id'],ratings['content_id']),ratings['rating']


def TrainingSet(data_set_path: str) -> tf.data.Dataset:
    pathname = os.path.join(data_set_path, "train", "part-r-*")
    return __DataSet(pathname=pathname)


def ValidationSet(data_set_path: str) -> tf.data.Dataset:
    pathname = os.path.join(data_set_path, "validation", "part-r-*")
    return __DataSet(pathname=pathname)


def TestSet(data_set_path: str) -> tf.data.Dataset:
    pathname = os.path.join(data_set_path, "test", "part-r-*")
    print(pathname)
    return __DataSet(pathname=pathname)



