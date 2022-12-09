import argparse
from os import path
from pyspark.sql import SparkSession
from typing import List

from src.feature_processor.pipelines.partition_ratings \
    import PartitionRatingGlobal
from src.feature_processor.pipelines.writer import \
    TFRECORDS_OUTPUT_FORMATTER_PATH
from src.feature_processor.pipelines.writer import WriteAsTfRecordDataSet
from src.feature_processor.pipelines.writer import WriteAsParquetDataSet
from src.ingestion.database.factory import IngestionReaderFactory


def Main(cassandra_contact_points: List[str],
         postgres_host: str,
         postgres_user: str,
         postgres_password: str,
         output_path: str) -> None:
    reader_factory = IngestionReaderFactory(
        cassandra_contact_points=cassandra_contact_points,
        postgres_host=postgres_host,
        postgres_user=postgres_user,
        postgres_password=postgres_password)

    builder = SparkSession.builder.appName("Feature Generator")
    builder = reader_factory.ConfigureSparkSession(
        spark_builder=builder,
        prerequisites_jars=[TFRECORDS_OUTPUT_FORMATTER_PATH])
    spark = builder.getOrCreate()
    spark.sparkContext.setCheckpointDir(path.join(output_path, "check_points"))

    reader = reader_factory.Create(spark=spark)

    train_set, valid_set, test_set = PartitionRatingGlobal(reader=reader)

    WriteAsParquetDataSet(
        df=train_set,
        output_path=path.join(
            output_path, "ratings", "parquet", "train"))
    WriteAsParquetDataSet(
        df=valid_set,
        output_path=path.join(
            output_path, "ratings", "parquet", "validation"))
    WriteAsParquetDataSet(
        df=test_set,
        output_path=path.join(
            output_path, "ratings", "parquet", "test"))

    WriteAsTfRecordDataSet(
        df=train_set,
        output_path=path.join(
            output_path, "ratings", "tfrecords", "train"))
    WriteAsTfRecordDataSet(
        df=valid_set,
        output_path=path.join(
            output_path, "ratings", "tfrecords", "validation"))
    WriteAsTfRecordDataSet(
        df=test_set,
        output_path=path.join(
            output_path, "ratings", "tfrecords", "test"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reads from the raw ingestion data set then partitions "
                    "the data records into training, validation and test sets."
                    " The data sets are then written out as tf-record data "
                    "sets.")
    parser.add_argument(
        "--cassandra_contact_points",
        nargs="+",
        type=str,
        help="The list of contact points to try connecting for Cassandra "
             "cluster discovery.")
    parser.add_argument(
        "--postgres_host",
        type=str,
        help="The IP address which points to the postgres database server "
             "which stores the raw ingestion data set.")
    parser.add_argument(
        "--postgres_user",
        type=str,
        help="The user to use to access the ingestion database.")
    parser.add_argument(
        "--postgres_password",
        type=str,
        help="The password of the postgres user.")
    parser.add_argument(
        "--output_path",
        type=str,
        help="Path where the training, validation and test data sets are "
             "going to be created.")

    args = parser.parse_args()

    if args.output_path is None:
        print("output_path is required.")
        exit(-1)

    Main(cassandra_contact_points=args.cassandra_contact_points,
         postgres_host=args.postgres_host,
         postgres_user=args.postgres_user,
         postgres_password=args.postgres_password,
         output_path=args.output_path)
