import argparse
import grpc
from concurrent import futures
from typing import List

from src.inference.proto_py.pointwise_ranking_service_pb2_grpc import \
    add_PointwiseRankingServicer_to_server
from src.inference.services.pointwise_ranking_service import \
    PointwiseRankingService


def __RunServer(grpc_port: int,
                pointwise_ranking_model_path: str,
                cassandra_contact_points: List[str]) -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    user_feedback_ingestion_service = PointwiseRankingService(
        model_path=pointwise_ranking_model_path,
        cassandra_contact_points=cassandra_contact_points)
    add_PointwiseRankingServicer_to_server(
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
        description="Launches a gRPC server which serves the ranking inference"
                    " services.")
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
        "--pointwise_ranking_model_path",
        type=str,
        help="The IP address which points to the postgres database server.")

    args = parser.parse_args()

    if args.grpc_port is None:
        print("grpc_port is required.")
        exit(-1)
    if args.cassandra_contact_points is None:
        print("cassandra_contact_points is required.")
        exit(-1)
    if args.pointwise_ranking_model_path is None:
        print("pointwise_ranking_model_path is required.")
        exit(-1)

    __RunServer(
        grpc_port=args.grpc_port,
        pointwise_ranking_model_path=args.pointwise_ranking_model_path,
        cassandra_contact_points=args.cassandra_contact_points)
