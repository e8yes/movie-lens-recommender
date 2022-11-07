import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reads from the raw ingestion data set then partitions the data records into training, validation and test sets. The data sets are then written out as tf-record data sets.")
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
        help="Path where the training, validation and test data sets are going to be created.")

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
