# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from . import pointwise_ranking_service_pb2 as pointwise__ranking__service__pb2


class PointwiseRankingStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.ComputePointwiseEstimates = channel.unary_unary(
                '/e8.PointwiseRanking/ComputePointwiseEstimates',
                request_serializer=pointwise__ranking__service__pb2.PointwiseEstimatesRequest.SerializeToString,
                response_deserializer=pointwise__ranking__service__pb2.PointwiseEstimatesResponse.FromString,
                )


class PointwiseRankingServicer(object):
    """Missing associated documentation comment in .proto file."""

    def ComputePointwiseEstimates(self, request, context):
        """Computes Pr(likes(Content[j]) | User[i], theta) in batch.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_PointwiseRankingServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'ComputePointwiseEstimates': grpc.unary_unary_rpc_method_handler(
                    servicer.ComputePointwiseEstimates,
                    request_deserializer=pointwise__ranking__service__pb2.PointwiseEstimatesRequest.FromString,
                    response_serializer=pointwise__ranking__service__pb2.PointwiseEstimatesResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'e8.PointwiseRanking', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class PointwiseRanking(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def ComputePointwiseEstimates(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/e8.PointwiseRanking/ComputePointwiseEstimates',
            pointwise__ranking__service__pb2.PointwiseEstimatesRequest.SerializeToString,
            pointwise__ranking__service__pb2.PointwiseEstimatesResponse.FromString,
            options, channel_credentials,
            call_credentials, compression, wait_for_ready, timeout, metadata)
