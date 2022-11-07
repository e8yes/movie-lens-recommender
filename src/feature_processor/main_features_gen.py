import argparse

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
        name_or_flags="--cassandra_seeds", type=str, nargs="+",
        help="Seed nodes to the Cassandra cluster where the user and content features are going to be written.")

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
    if args.cassandra_seeds is None:
        print("cassandra_seeds is required.")
        exit(-1)
