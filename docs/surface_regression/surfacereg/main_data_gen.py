import argparse
import os
import sys

from surfacereg.data_gen.generator import CreateSparkSession, GenerateDataSet


def __Generate(surface_type: str,
               sample_count: int,
               noise_level: float,
               output_path: str) -> None:
    spark = CreateSparkSession()

    training_set = max(1, int(sample_count*0.8))
    validation_set = max(1, int(sample_count*0.1))
    test_set = max(1, sample_count - (training_set + validation_set))

    GenerateDataSet(
        surface_type=surface_type,
        sample_count=training_set,
        noise_level=noise_level,
        spark=spark,
        output_path=os.path.join(output_path, "training"))
    GenerateDataSet(
        surface_type=surface_type,
        sample_count=validation_set,
        noise_level=noise_level,
        spark=spark,
        output_path=os.path.join(output_path, "validation"))
    GenerateDataSet(
        surface_type=surface_type,
        sample_count=test_set,
        noise_level=noise_level,
        spark=spark,
        output_path=os.path.join(output_path, "test"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generates representative samples of a surface embedded in R^3.")
    parser.add_argument(
        "--surface_type", type=str,
        help="The type of surface to generate. Value can be chosen from \{cloth, morbius_strip\}.")
    parser.add_argument(
        "--sample_count", type=int,
        help="The total number of samples to generate.")
    parser.add_argument(
        "--noise_level", type=float,
        help="The standard deviation of the Gaussian noise added to the samples.")
    parser.add_argument(
        "--output_path", type=str,
        help="Directory to which the generated data set is saved.")

    args = parser.parse_args()
    if args.surface_type is None:
        args.surface_type = "cloth"
    if args.sample_count is None:
        args.sample_count = 100000
    if args.noise_level is None:
        args.noise_level = 0.0
    if args.output_path is None:
        print("output_path is required.")
        exit(-1)

    __Generate(surface_type=args.surface_type,
               sample_count=args.sample_count,
               noise_level=args.noise_level,
               output_path=args.output_path)
