import argparse

from surfacereg.modeling.train import TrainModel

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Creates and trains a surface model.")
    parser.add_argument(
        "--data_set_path", type=str,
        help="Directory of the data set on which the model is going to train.")
    parser.add_argument(
        "--model_type", type=str,
        help="The type of model to create.")
    parser.add_argument(
        "--output_path", type=str,
        help="Directory to which the trained model is saved.")

    args = parser.parse_args()
    if args.data_set_path is None:
        print("data_set_path is required.")
        exit(-1)
    if args.model_type is None:
        print("model_type is required.")
        exit(-1)
    if args.output_path is None:
        print("output_path is required.")
        exit(-1)

    model = TrainModel(model_type=args.model_type,
                       data_set_path=args.data_set_path)
    model.save(filepath=args.output_path)
