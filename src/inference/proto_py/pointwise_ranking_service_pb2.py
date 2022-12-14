# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: pointwise_ranking_service.proto

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='pointwise_ranking_service.proto',
  package='e8',
  syntax='proto3',
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\x1fpointwise_ranking_service.proto\x12\x02\x65\x38\"B\n\x19PointwiseEstimatesRequest\x12\x10\n\x08user_ids\x18\x01 \x03(\x03\x12\x13\n\x0b\x63ontent_ids\x18\x02 \x03(\x03\"+\n\x1aPointwiseEstimatesResponse\x12\r\n\x05probs\x18\x01 \x03(\x02\x32n\n\x10PointwiseRanking\x12Z\n\x19\x43omputePointwiseEstimates\x12\x1d.e8.PointwiseEstimatesRequest\x1a\x1e.e8.PointwiseEstimatesResponseb\x06proto3'
)




_POINTWISEESTIMATESREQUEST = _descriptor.Descriptor(
  name='PointwiseEstimatesRequest',
  full_name='e8.PointwiseEstimatesRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='user_ids', full_name='e8.PointwiseEstimatesRequest.user_ids', index=0,
      number=1, type=3, cpp_type=2, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='content_ids', full_name='e8.PointwiseEstimatesRequest.content_ids', index=1,
      number=2, type=3, cpp_type=2, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=39,
  serialized_end=105,
)


_POINTWISEESTIMATESRESPONSE = _descriptor.Descriptor(
  name='PointwiseEstimatesResponse',
  full_name='e8.PointwiseEstimatesResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='probs', full_name='e8.PointwiseEstimatesResponse.probs', index=0,
      number=1, type=2, cpp_type=6, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=107,
  serialized_end=150,
)

DESCRIPTOR.message_types_by_name['PointwiseEstimatesRequest'] = _POINTWISEESTIMATESREQUEST
DESCRIPTOR.message_types_by_name['PointwiseEstimatesResponse'] = _POINTWISEESTIMATESRESPONSE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

PointwiseEstimatesRequest = _reflection.GeneratedProtocolMessageType('PointwiseEstimatesRequest', (_message.Message,), {
  'DESCRIPTOR' : _POINTWISEESTIMATESREQUEST,
  '__module__' : 'pointwise_ranking_service_pb2'
  # @@protoc_insertion_point(class_scope:e8.PointwiseEstimatesRequest)
  })
_sym_db.RegisterMessage(PointwiseEstimatesRequest)

PointwiseEstimatesResponse = _reflection.GeneratedProtocolMessageType('PointwiseEstimatesResponse', (_message.Message,), {
  'DESCRIPTOR' : _POINTWISEESTIMATESRESPONSE,
  '__module__' : 'pointwise_ranking_service_pb2'
  # @@protoc_insertion_point(class_scope:e8.PointwiseEstimatesResponse)
  })
_sym_db.RegisterMessage(PointwiseEstimatesResponse)



_POINTWISERANKING = _descriptor.ServiceDescriptor(
  name='PointwiseRanking',
  full_name='e8.PointwiseRanking',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=152,
  serialized_end=262,
  methods=[
  _descriptor.MethodDescriptor(
    name='ComputePointwiseEstimates',
    full_name='e8.PointwiseRanking.ComputePointwiseEstimates',
    index=0,
    containing_service=None,
    input_type=_POINTWISEESTIMATESREQUEST,
    output_type=_POINTWISEESTIMATESRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_POINTWISERANKING)

DESCRIPTOR.services_by_name['PointwiseRanking'] = _POINTWISERANKING

# @@protoc_insertion_point(module_scope)
