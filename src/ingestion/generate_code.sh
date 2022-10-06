#!/bin/bash

# Generates gRPC and protobuf python outputs.
protoc --python_out=proto_py                                          \
       --grpc_out=proto_py                                            \
       --plugin=protoc-gen-grpc=`which grpc_python_plugin`            \
       --proto_path=proto                                             \
       `find proto -name *.proto`

# Turns top-level absolute imports to local relative imports.
find proto_py -type f -name "*.py" -exec                              \
       sed -i 's/import \(..*\)_pb2/from . import \1_pb2/g' {} \;
