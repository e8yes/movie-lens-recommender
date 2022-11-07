import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=".")
    parser.add_argument(
        name_or_flags="--model_path", type=str, nargs="+",
        help="Path to a trained model through which user and content embeddings can be calculated.")
    parser.add_argument(
        name_or_flags="--cassandra_seeds", type=str, nargs="+",
        help="Seed nodes to the Cassandra cluster where the user and content features are stored.")

    args = parser.parse_args()

    if args.model_path is None:
        print("model_path is required.")
        exit(-1)
    if args.cassandra_seeds is None:
        print("cassandra_seeds is required.")
        exit(-1)
