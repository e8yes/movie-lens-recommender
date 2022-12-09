from pyspark.sql import DataFrame, SparkSession


def WriteAsTfRecordDataSet(df: DataFrame,
                           spark: SparkSession,
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
