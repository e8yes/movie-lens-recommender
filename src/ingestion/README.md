# Ingestion services
Design doc: https://docs.google.com/document/d/1h4WPJDB1XV3-0rYfTvzqV6r28yvEmMsCnd2SP0b3VIQ/edit?usp=sharing

## Preparation
 - Supported OS: Ubuntu 22.04
 - Install pip ```apt install python3-pip```
 - Install protobuf compiler ```apt install protobuf-compiler-grpc```
 - Install python protobuf package ```pip3 install protobuf```
 - Install python gRPC package ```pip3 install grpcio```
 - Generate code ```./generate_code.sh ```
 - Set up the ingestion database ```./push_schema.sh```
