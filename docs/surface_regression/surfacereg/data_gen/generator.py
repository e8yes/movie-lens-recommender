import numpy as np
from pyspark import Row
from pyspark.sql import SparkSession, types
from typing import Iterable

from surfacereg.data_gen.surface_cloth import GenerateCloth
from surfacereg.data_gen.surface_morbius_strip import GenerateMorbiusStrip


#
SURFACE_TYPE_CLOTH = "cloth"
SURFACE_TYPE_MORBIUS_STRIP = "morbius_strip"

__BATCH_COUNT = 12


def __GenerateBatch(surface_type: str,
                    sample_count: int,
                    noise_level: float) -> Iterable[Row]:
    samples: np.ndarray = np.array([])
    if surface_type == SURFACE_TYPE_CLOTH:
        samples = GenerateCloth(sample_count=sample_count,
                                noise_level=noise_level)
    elif surface_type == SURFACE_TYPE_MORBIUS_STRIP:
        samples = GenerateMorbiusStrip(sample_count=sample_count,
                                       noise_level=noise_level)
    else:
        raise "Unknown surface type: " + surface_type

    for i in range(samples.shape[0]):
        x1 = float(samples[i, 0])
        x2 = float(samples[i, 1])
        z = float(samples[i, 2])

        yield Row(X=[x1, x2], Z=z)


def GenerateDataSet(surface_type: str,
                    sample_count: int,
                    noise_level: float,
                    spark: SparkSession,
                    output_path: str):
    """_summary_

    Args:
        surface_type (str): _description_
        sample_count (int): _description_
        noise_level (float): _description_
        spark (SparkSession): _description_
        output_path (str): _description_
    """
    per_batch_sample_counts = [sample_count//__BATCH_COUNT]*__BATCH_COUNT
    per_batch_sample_counts[-1] = sample_count - \
        per_batch_sample_counts[0]*(__BATCH_COUNT - 1)

    rdd = spark.sparkContext.parallelize(per_batch_sample_counts)
    samples = rdd.flatMap(
        lambda sample_count: __GenerateBatch(surface_type,
                                             sample_count,
                                             noise_level))

    schema = types.StructType(
        [
            types.StructField("X", types.ArrayType(types.FloatType())),
            types.StructField("Z", types.FloatType()),
        ])
    data_set = samples.toDF(schema=schema)
    data_set.show()

    try:
        data_set.write.\
            format("tfrecords").\
            option("recordType", "Example").\
            option("codec", "org.apache.hadoop.io.compress.GzipCodec").\
            mode("overwrite").\
            save(path=output_path)
    except:
        pass


def CreateSparkSession() -> SparkSession:
    """_summary_

    Returns:
        SparkSession: _description_
    """
    spark = SparkSession.\
        builder.\
        appName("Morbius Strip Gen").\
        config("spark.jars", "third_party/spark-tensorflow-connector_2.11-1.15.0.jar").\
        getOrCreate()
    return spark
