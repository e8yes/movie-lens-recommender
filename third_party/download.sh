#!/bin/bash

echo "==============================================="
echo "Setting up GloVe 6B word embeddings definitions..."
wget https://downloads.cs.stanford.edu/nlp/data/glove.6B.zip
unzip -o glove.6B.zip -d glove
rm glove.6B.zip
echo "==============================================="

echo "==============================================="
echo "Setting up NLTK data..."
python3 download_nltk.py
echo "==============================================="

echo "All third party data is ready to use."
