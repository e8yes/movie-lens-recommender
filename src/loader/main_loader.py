import argparse

from src.loader.pipeline import LoadMovieLensDataSet

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Loads existing data sets into the recommender system "
                    "using the ingestion services.")
    parser.add_argument(
        "--data_set_path",
        type=str,
        help="The directory path to the root of the existing data sets.")
    parser.add_argument(
        "--ingestion_host",
        type=str,
        help="The host address which points to the ingestion gRPC server.")
    parser.add_argument(
        "--feedback_host",
        type=str,
        help="The host address which points to the user feedback gRPC server.")

    args = parser.parse_args()

    if args.data_set_path is None:
        print("data_set_path is required.")
        exit(-1)
    if args.ingestion_host is None:
        print("ingestion_host is required.")
        exit(-1)
    if args.feedback_host is None:
        print("feedback_host is required.")
        exit(-1)

    LoadMovieLensDataSet(data_set_path=args.data_set_path,
                         ingestion_host=args.ingestion_host,
                         feedback_host=args.feedback_host)
