import argparse
from os import path

from src.feature_processor.pipelines.writer import WriteAsTfRecordDataSet
from src.feature_processor.pipelines.assemble_features import AssembleContentFeatures
from src.feature_processor.pipelines.assemble_features import AssembleUserFeatures
from src.ingestion.database.reader import IngestionReader


def Main(postgres_host: str,
         postgres_user: str,
         postgres_password: str,
         output_path: str):
    reader = IngestionReader(db_host=postgres_host,
                             db_user=postgres_user,
                             db_password=postgres_password)

    content_features = AssembleContentFeatures(reader=reader)
    user_features = AssembleUserFeatures(reader=reader)

    WriteAsTfRecordDataSet(
        df=content_features, spark=reader.spark_session, output_path=path.join(
            output_path, "content_features"))
    WriteAsTfRecordDataSet(df=user_features,
                           spark=reader.spark_session,
                           output_path=path.join(output_path, "user_features"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extracts and transforms information drawn from the raw data set to create user and content features for modeling.")
    parser.add_argument(
        name_or_flags="--postgres_host", type=str,
        help="The IP address which points to the postgres database server which stores the raw ingestion data set.")
    parser.add_argument(
        name_or_flags="--postgres_user",
        type=str,
        help="The user to use to access the ingestion database.")
    parser.add_argument(
        name_or_flags="--postgres_password",
        type=str,
        help="The password of the postgres user.")
    parser.add_argument(
        name_or_flags="--output_path", type=str, nargs="+",
        help="Path where the user and content features are going to be written.")

    args = parser.parse_args()

    if args.postgres_host is None:
        print("postgres_host is required.")
        exit(-1)
    if args.postgres_user is None:
        print("postgres_user is required.")
        exit(-1)
    if args.postgres_password is None:
        print("postgres_password is required.")
        exit(-1)
    if args.output_path is None:
        print("output_path is required.")
        exit(-1)

    Main(postgres_host=args.postgres_host,
         postgres_user=args.postgres_user,
         postgres_password=args.postgres_password,
         output_path=args.output_path)
