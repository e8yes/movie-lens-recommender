# Data set loader
Design doc: https://docs.google.com/document/d/1h4WPJDB1XV3-0rYfTvzqV6r28yvEmMsCnd2SP0b3VIQ/edit?usp=sharing

## Preparation
 - Supported OS: Ubuntu 22.04
 - Do all the preparation steps in src/ingestion.
 - Install wget ```apt install wget```
 - Install python spark package ```pip3 install pyspark```
 - Read the readme file at src/loader/datasets/movie_lens/README.txt, then download the movie-lens data set by running ```cd datasets/movie_lens && ./download.sh && cd ../../```
