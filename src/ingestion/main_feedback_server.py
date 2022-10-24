import argparse
import grpc
from concurrent import futures
from typing import Any

from src.ingestion.database.common import CreateConnection
from src.ingestion.proto_py.user_feedback_ingestion_service_pb2_grpc import add_UserFeedbackIngestionServicer_to_server
from src.ingestion.services.user_feedback_service import UserFeedbackIngestionService


def __RunServer(grpc_port: int, pg_conn: Any):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    user_feedback_ingestion_service = UserFeedbackIngestionService(
        pg_conn=pg_conn)
    add_UserFeedbackIngestionServicer_to_server(
        servicer=user_feedback_ingestion_service, server=server)

    server.add_insecure_port("[::]:{port}".format(port=grpc_port))
    server.start()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=None)
        server.wait_for_termination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Launches a gRPC server which serves the user feedback ingestion services.")
    parser.add_argument("--grpc_port",
                        type=int,
                        help="The port number on which the gRPC server runs.")
    parser.add_argument(
        "--postgres_host", type=str,
        help="The IP address which points to the postgres database server.")
    parser.add_argument("--postgres_password",
                        type=str,
                        help="The password of the postgres database user.")

    args = parser.parse_args()

    if args.grpc_port is None:
        print("grpc_port is required.")
        exit(-1)
    if args.postgres_host is None:
        print("postgres_host is required.")
        exit(-1)
    if args.postgres_password is None:
        print("postgres_password is required.")
        exit(-1)

    pg_conn = CreateConnection(host=args.postgres_host,
                               password=args.postgres_password)
    __RunServer(grpc_port=args.grpc_port, pg_conn=pg_conn)
