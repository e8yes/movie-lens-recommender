#!/bin/bash

dataset="ml-latest"

echo "==============================================="
echo "Please see ml-latest-README.html for licensing."

wget "https://files.grouplens.org/datasets/movielens/$dataset.zip"
unzip $dataset.zip
rm $dataset.zip
mv $dataset/* .
rm -r $dataset

echo "The dataset is ready to use."
echo "==============================================="

