import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Creates and trains a point-wise personalized content ranking model on the rating data set.")
    parser.add_argument(
        name_or_flags="--cassandra_seeds", type=str, nargs="+",
        help="Seed nodes to the Cassandra cluster where the user and content features are stored.")
    parser.add_argument(
        name_or_flags="--rating_data_set_path", type=str, nargs="+",
        help="Path in which the rating data sets are stored.")
    parser.add_argument(
        name_or_flags="--model_output_path", type=str, nargs="+",
        help="Path where the trained model is going to be created.")

    args = parser.parse_args()

    if args.cassandra_seeds is None:
        print("cassandra_seeds is required.")
        exit(-1)
    if args.rating_data_set_path is None:
        print("rating_data_set_path is required.")
        exit(-1)
    if args.model_output_path is None:
        print("model_output_path is required.")
        exit(-1)
