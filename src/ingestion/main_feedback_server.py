import argparse
import grpc
from concurrent import futures

from src.ingestion.database.factory import IngestionWriterFactory
from src.ingestion.proto_py.user_feedback_ingestion_service_pb2_grpc import \
    add_UserFeedbackIngestionServicer_to_server
from src.ingestion.services.user_feedback_service import \
    UserFeedbackIngestionService


def __RunServer(
        grpc_port: int, writer_factory: IngestionWriterFactory) -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    ingestion_writer = writer_factory.Create()

    user_feedback_ingestion_service = UserFeedbackIngestionService(
        writer=ingestion_writer)
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
        description="Launches a gRPC server which serves the user feedback "
                    "ingestion services.")
    parser.add_argument("--grpc_port",
                        type=int,
                        help="The port number on which the gRPC server runs.")
    parser.add_argument(
        "--cassandra_contact_points",
        nargs="+",
        type=str,
        help="The list of contact points to try connecting for Cassandra "
             "cluster discovery.")
    parser.add_argument(
        "--postgres_host",
        type=str,
        help="The IP address which points to the postgres database server.")
    parser.add_argument(
        "--postgres_password",
        type=str,
        help="The password of the postgres database user.")

    args = parser.parse_args()

    if args.grpc_port is None:
        print("grpc_port is required.")
        exit(-1)

    writer_factory = IngestionWriterFactory(
        cassandra_contact_points=args.cassandra_contact_points,
        postgres_host=args.postgres_host,
        postgres_password=args.postgres_password)
    __RunServer(grpc_port=args.grpc_port, writer_factory=writer_factory)
