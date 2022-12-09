from pyspark.sql import DataFrame

TFRECORDS_OUTPUT_FORMATTER_PATH = \
    "third_party/spark-tensorflow-connector_2.11-1.15.0.jar"


def WriteAsTfRecordDataSet(df: DataFrame,
                           output_path: str) -> None:
    try:
        df.write.\
            format("tfrecords").\
            option("recordType", "Example").\
            option("codec", "org.apache.hadoop.io.compress.GzipCodec").\
            mode("overwrite").\
            save(path=output_path)
    except Exception:
        pass


def WriteAsParquetDataSet(df: DataFrame, output_path: str) -> None:
    df.write.parquet(path=output_path, mode="overwrite")
