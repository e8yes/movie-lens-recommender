#!/bin/bash

protoc --python_out=proto_py \
       --grpc_out=proto_py \
       --plugin=protoc-gen-grpc=`which grpc_python_plugin` \
       --proto_path=proto \
       `find proto -name *.proto`
