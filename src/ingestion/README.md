# Ingestion services
Design doc: https://docs.google.com/document/d/1h4WPJDB1XV3-0rYfTvzqV6r28yvEmMsCnd2SP0b3VIQ/edit?usp=sharing

## Preparation
 - Supported OS: Ubuntu 22.04
 - Install Java 8 ```apt install openjdk-8-jdk``` then set the JAVA_HOME environment variable by adding the line ```export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64``` to ```~/.bashrc```
 - Install Cassandra DBMS ```apt install cassandra```. See here ```https://cassandra.apache.org/doc/latest/cassandra/getting_started/installing.html#installing-the-debian-packages``` to obtain the APT source.
 - Install cassandra python driver package ```pip3 install cassandra-driver```
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

## Main programs
 - This program runs the XMDB crawler: ```python3 -m src.ingestion.main_crawler --kafka_host="IP address of the Kafka host" --cassandra_contact_points="A list of cassandra contact points" ----tmdb_api_key="TMDB developer API key"```
 - This program runs the ingestion server: ```python3 -m src.ingestion.main_ingestion_server --grpc_port="The port the server is going to run on" --kafka_host="IP address of the Kafka host"```
 - This program runs the feedback server: ```python3 -m src.ingestion.main_feedback_server --grpc_port="The port the server is going to run on"```
