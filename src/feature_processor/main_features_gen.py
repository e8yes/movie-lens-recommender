import argparse
from os import path
from pyspark.sql import SparkSession
from typing import List

from src.feature_processor.pipelines.compute_features \
    import ComputeContentFeatures
from src.feature_processor.pipelines.compute_features \
    import ComputeUserFeatures
from src.feature_processor.pipelines.writer import WriteAsParquetDataSet
from src.ingestion.database.factory import IngestionReaderFactory


def Main(cassandra_contact_points: List[str],
         postgres_host: str,
         postgres_user: str,
         postgres_password: str,
         output_path: str):
    reader_factory = IngestionReaderFactory(
        cassandra_contact_points=cassandra_contact_points,
        postgres_host=postgres_host,
        postgres_user=postgres_user,
        postgres_password=postgres_password)

    builder = SparkSession.builder.appName("Feature Generator")
    builder = reader_factory.ConfigureSparkSession(spark_builder=builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setCheckpointDir(path.join(output_path, "check_points"))

    reader = reader_factory.Create(spark=spark)

    content_features = ComputeContentFeatures(reader=reader, spark=spark)
    user_features = ComputeUserFeatures(reader=reader)

    WriteAsParquetDataSet(
        df=content_features,
        output_path=path.join(output_path, "content_features"))
    WriteAsParquetDataSet(
        df=user_features,
        output_path=path.join(output_path, "user_features"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extracts and transforms information drawn from the raw "
        "data set to create user and content features for modeling.")
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
        help="Path where the user and content features are going to be written"
        ".")

    args = parser.parse_args()

    if args.output_path is None:
        print("output_path is required.")
        exit(-1)

    Main(cassandra_contact_points=args.cassandra_contact_points,
         postgres_host=args.postgres_host,
         postgres_user=args.postgres_user,
         postgres_password=args.postgres_password,
         output_path=args.output_path)
