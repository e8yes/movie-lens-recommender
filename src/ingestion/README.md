# Ingestion services
Design doc: https://docs.google.com/document/d/1h4WPJDB1XV3-0rYfTvzqV6r28yvEmMsCnd2SP0b3VIQ/edit?usp=sharing

## Preparation
 - Supported OS: Ubuntu 22.04
 - Install Java 8 ```apt install openjdk-8-jdk``` then set the JAVA_HOME environment variable by adding the line ```export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64``` to ```~/.bashrc```
 - Install Cassandra DBMS ```apt install cassandra```. See here ```https://cassandra.apache.org/doc/latest/cassandra/getting_started/installing.html#installing-the-debian-packages``` to obtain the APT source.
 - Install postgresql DBMS ```apt install postgresql```
 - Install postgres python driver package ```apt install libpq-dev``` and ```pip3 install psycopg2```
 - Install pip ```apt install python3-pip```
 - Install protobuf compiler ```apt install protobuf-compiler-grpc```
 - Install python protobuf package ```pip3 install protobuf```
 - Install python gRPC package ```pip3 install grpcio```
 - Install python Kafka package ```pip3 install kafka-python```
 - Install python TMDB wrapper package ```pip3 install tmdbsimple```
 - Generate code ```./generate_code.sh ```
 - Set up the ingestion database ```./push_schema.sh```
