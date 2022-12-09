import argparse
import grpc
from concurrent import futures

from src.ingestion.database.factory import IngestionWriterFactory
from src.ingestion.proto_py.content_ingestion_service_pb2_grpc import \
    add_ContentIngestionServicer_to_server
from src.ingestion.proto_py.user_ingestion_service_pb2_grpc import \
    add_UserIngestionServicer_to_server
from src.ingestion.services.content_profile_service import \
    ContentIngestionService
from src.ingestion.services.user_profile_service import UserIngestionService


def __RunServer(grpc_port: int,
                writer_factory: IngestionWriterFactory,
                kafka_host: str):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    ingestion_writer = writer_factory.Create()

    user_ingestion_service = UserIngestionService(writer=ingestion_writer)
    content_ingestion_service = ContentIngestionService(
        writer=ingestion_writer, kafka_host=kafka_host)

    add_UserIngestionServicer_to_server(
        servicer=user_ingestion_service, server=server)
    add_ContentIngestionServicer_to_server(
        servicer=content_ingestion_service, server=server)

    server.add_insecure_port("[::]:{port}".format(port=grpc_port))
    server.start()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=None)
        server.wait_for_termination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Launches a gRPC server which serves the ingestion "
                    "services.")
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
        "--postgres_host", type=str,
        help="The IP address which points to the postgres database server.")
    parser.add_argument(
        "--postgres_password",
        type=str,
        help="The password of the postgres database user.")
    parser.add_argument(
        "--kafka_host", type=str,
        help="The host address (with port number) which points to the Kafka "
             "server.")

    args = parser.parse_args()

    if args.grpc_port is None:
        print("grpc_port is required.")
        exit(-1)
    if args.kafka_host is None:
        print("kafka_host is required.")
        exit(-1)

    writer_factory = IngestionWriterFactory(
        cassandra_contact_points=args.cassandra_contact_points,
        postgres_host=args.postgres_host,
        postgres_password=args.postgres_password)

    __RunServer(grpc_port=args.grpc_port,
                writer_factory=writer_factory,
                kafka_host=args.kafka_host)
