"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_sym_db = _symbol_database.Default()
from .....neptune_pb.ingest.v1.pub import request_status_pb2 as neptune__pb_dot_ingest_dot_v1_dot_pub_dot_request__status__pb2
from .....neptune_pb.ingest.v1 import ingest_pb2 as neptune__pb_dot_ingest_dot_v1_dot_ingest__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n%neptune_pb/ingest/v1/pub/client.proto\x12\x15neptune.ingest.v1.pub\x1a-neptune_pb/ingest/v1/pub/request_status.proto\x1a!neptune_pb/ingest/v1/ingest.proto"\x1a\n\tRequestId\x12\r\n\x05value\x18\x01 \x01(\t">\n\rRequestIdList\x12-\n\x03ids\x18\x01 \x03(\x0b2 .neptune.ingest.v1.pub.RequestId"K\n\x11BulkRequestStatus\x126\n\x08statuses\x18\x01 \x03(\x0b2$.neptune.ingest.v1.pub.RequestStatus"9\n\x0eSubmitResponse\x12\x12\n\nrequest_id\x18\x01 \x01(\t\x12\x13\n\x0brequest_ids\x18\x02 \x03(\t"T\n\x0bStatusCheck\x12\x0f\n\x07project\x18\x01 \x01(\t\x124\n\nrequest_id\x18\x02 \x01(\x0b2 .neptune.ingest.v1.pub.RequestId"Q\n\x0fBulkStatusCheck\x12\x0f\n\x07project\x18\x02 \x01(\t\x12-\n\x03ids\x18\x01 \x03(\x0b2 .neptune.ingest.v1.pub.RequestId"\x99\x01\n\x0fBulkCheckDetail\x12\x0f\n\x07project\x18\x01 \x01(\t\x12-\n\x03ids\x18\x02 \x03(\x0b2 .neptune.ingest.v1.pub.RequestId\x12\x18\n\x0bnext_cursor\x18\x03 \x01(\tH\x00\x88\x01\x01\x12\x12\n\x05limit\x18\x04 \x01(\x05H\x01\x88\x01\x01B\x0e\n\x0c_next_cursorB\x08\n\x06_limit"\xa2\x01\n\x12IngestResultDetail\x124\n\nrequest_id\x18\x01 \x01(\x0b2 .neptune.ingest.v1.pub.RequestId\x12\x0c\n\x04path\x18\x02 \x01(\t\x122\n\x0bingest_code\x18\x03 \x01(\x0e2\x1d.neptune.ingest.v1.IngestCode\x12\x14\n\x0cerror_detail\x18\x04 \x01(\t"i\n\x16BulkIngestResultDetail\x12:\n\x07details\x18\x01 \x03(\x0b2).neptune.ingest.v1.pub.IngestResultDetail\x12\x13\n\x0bnext_cursor\x18\x02 \x01(\tB3\n\x1cml.neptune.client.api.modelsB\x11ClientIngestProtoP\x01b\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'neptune_pb.ingest.v1.pub.client_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
    _globals['DESCRIPTOR']._options = None
    _globals['DESCRIPTOR']._serialized_options = b'\n\x1cml.neptune.client.api.modelsB\x11ClientIngestProtoP\x01'
    _globals['_REQUESTID']._serialized_start = 146
    _globals['_REQUESTID']._serialized_end = 172
    _globals['_REQUESTIDLIST']._serialized_start = 174
    _globals['_REQUESTIDLIST']._serialized_end = 236
    _globals['_BULKREQUESTSTATUS']._serialized_start = 238
    _globals['_BULKREQUESTSTATUS']._serialized_end = 313
    _globals['_SUBMITRESPONSE']._serialized_start = 315
    _globals['_SUBMITRESPONSE']._serialized_end = 372
    _globals['_STATUSCHECK']._serialized_start = 374
    _globals['_STATUSCHECK']._serialized_end = 458
    _globals['_BULKSTATUSCHECK']._serialized_start = 460
    _globals['_BULKSTATUSCHECK']._serialized_end = 541
    _globals['_BULKCHECKDETAIL']._serialized_start = 544
    _globals['_BULKCHECKDETAIL']._serialized_end = 697
    _globals['_INGESTRESULTDETAIL']._serialized_start = 700
    _globals['_INGESTRESULTDETAIL']._serialized_end = 862
    _globals['_BULKINGESTRESULTDETAIL']._serialized_start = 864
    _globals['_BULKINGESTRESULTDETAIL']._serialized_end = 969