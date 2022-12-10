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
  serialized_pb=b'\n\x1fpointwise_ranking_service.proto\x12\x02\x65\x38\"?\n\x18PointwiseEstimateRequest\x12\x0f\n\x07user_id\x18\x01 \x01(\x03\x12\x12\n\ncontent_id\x18\x02 \x01(\x03\")\n\x19PointwiseEstimateResponse\x12\x0c\n\x04prob\x18\x01 \x01(\x02\x32k\n\x10PointwiseRanking\x12W\n\x18\x43omputePointwiseEstimate\x12\x1c.e8.PointwiseEstimateRequest\x1a\x1d.e8.PointwiseEstimateResponseb\x06proto3'
)




_POINTWISEESTIMATEREQUEST = _descriptor.Descriptor(
  name='PointwiseEstimateRequest',
  full_name='e8.PointwiseEstimateRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='user_id', full_name='e8.PointwiseEstimateRequest.user_id', index=0,
      number=1, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='content_id', full_name='e8.PointwiseEstimateRequest.content_id', index=1,
      number=2, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
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
  serialized_end=102,
)


_POINTWISEESTIMATERESPONSE = _descriptor.Descriptor(
  name='PointwiseEstimateResponse',
  full_name='e8.PointwiseEstimateResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='prob', full_name='e8.PointwiseEstimateResponse.prob', index=0,
      number=1, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
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
  serialized_start=104,
  serialized_end=145,
)

DESCRIPTOR.message_types_by_name['PointwiseEstimateRequest'] = _POINTWISEESTIMATEREQUEST
DESCRIPTOR.message_types_by_name['PointwiseEstimateResponse'] = _POINTWISEESTIMATERESPONSE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

PointwiseEstimateRequest = _reflection.GeneratedProtocolMessageType('PointwiseEstimateRequest', (_message.Message,), {
  'DESCRIPTOR' : _POINTWISEESTIMATEREQUEST,
  '__module__' : 'pointwise_ranking_service_pb2'
  # @@protoc_insertion_point(class_scope:e8.PointwiseEstimateRequest)
  })
_sym_db.RegisterMessage(PointwiseEstimateRequest)

PointwiseEstimateResponse = _reflection.GeneratedProtocolMessageType('PointwiseEstimateResponse', (_message.Message,), {
  'DESCRIPTOR' : _POINTWISEESTIMATERESPONSE,
  '__module__' : 'pointwise_ranking_service_pb2'
  # @@protoc_insertion_point(class_scope:e8.PointwiseEstimateResponse)
  })
_sym_db.RegisterMessage(PointwiseEstimateResponse)



_POINTWISERANKING = _descriptor.ServiceDescriptor(
  name='PointwiseRanking',
  full_name='e8.PointwiseRanking',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=147,
  serialized_end=254,
  methods=[
  _descriptor.MethodDescriptor(
    name='ComputePointwiseEstimate',
    full_name='e8.PointwiseRanking.ComputePointwiseEstimate',
    index=0,
    containing_service=None,
    input_type=_POINTWISEESTIMATEREQUEST,
    output_type=_POINTWISEESTIMATERESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_POINTWISERANKING)

DESCRIPTOR.services_by_name['PointwiseRanking'] = _POINTWISERANKING

# @@protoc_insertion_point(module_scope)
