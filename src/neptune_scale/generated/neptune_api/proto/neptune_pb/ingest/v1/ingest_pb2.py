"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_sym_db = _symbol_database.Default()
from ....google_rpc import code_pb2 as google__rpc_dot_code__pb2
from ....neptune_pb.ingest.v1 import common_pb2 as neptune__pb_dot_ingest_dot_v1_dot_common__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n!neptune_pb/ingest/v1/ingest.proto\x12\x11neptune.ingest.v1\x1a\x15google_rpc/code.proto\x1a!neptune_pb/ingest/v1/common.proto"0\n\x0cBatchContext\x12\x0f\n\x07project\x18\x01 \x01(\t\x12\x0f\n\x07api_key\x18\x02 \x01(\x0c"V\n\tUpdateRun\x12A\n\x10update_snapshots\x18\x08 \x01(\x0b2%.neptune.ingest.v1.UpdateRunSnapshotsH\x00B\x06\n\x04mode"\xb8\x02\n\x16BatchProjectOperations\x120\n\x07context\x18\x01 \x01(\x0b2\x1f.neptune.ingest.v1.BatchContext\x12\x1e\n\x16create_missing_project\x18\x03 \x01(\x08\x12+\n\x0bcreate_runs\x18\x04 \x03(\x0b2\x16.neptune.ingest.v1.Run\x12N\n\x0bupdate_runs\x18\x08 \x03(\x0b29.neptune.ingest.v1.BatchProjectOperations.UpdateRunsEntry\x1aO\n\x0fUpdateRunsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12+\n\x05value\x18\x02 \x01(\x0b2\x1c.neptune.ingest.v1.UpdateRun:\x028\x01"z\n\x0eCreateRunError\x12#\n\tgrpc_code\x18\x01 \x01(\x0e2\x10.google_rpc.Code\x122\n\x0bingest_code\x18\x03 \x01(\x0e2\x1d.neptune.ingest.v1.IngestCode\x12\x0f\n\x07message\x18\x02 \x01(\t"t\n\x0fCreateRunResult\x12%\n\x03run\x18\x01 \x01(\x0b2\x16.neptune.ingest.v1.RunH\x00\x122\n\x05error\x18\x02 \x01(\x0b2!.neptune.ingest.v1.CreateRunErrorH\x00B\x06\n\x04type"\xb4\x03\n\rResultSummary\x12\x1e\n\x16total_operations_count\x18\x01 \x01(\x03\x12\x18\n\x10successful_count\x18\x02 \x01(\x03\x12\x14\n\x0cfailed_count\x18\x03 \x01(\x03\x12V\n\x13failed_by_grpc_code\x18\x04 \x03(\x0b29.neptune.ingest.v1.ResultSummary.FailedByGRPCCodeCounters\x12R\n\x15failed_by_ingest_code\x18\x05 \x03(\x0b23.neptune.ingest.v1.ResultSummary.IngestCodeCounters\x1aN\n\x18FailedByGRPCCodeCounters\x12#\n\tgrpc_code\x18\x01 \x01(\x0e2\x10.google_rpc.Code\x12\r\n\x05count\x18\x02 \x01(\x03\x1aW\n\x12IngestCodeCounters\x122\n\x0bingest_code\x18\x01 \x01(\x0e2\x1d.neptune.ingest.v1.IngestCode\x12\r\n\x05count\x18\x02 \x01(\x03"\x89\x01\n\x0eUpdateRunError\x12#\n\tgrpc_code\x18\x01 \x01(\x0e2\x10.google_rpc.Code\x122\n\x0bingest_code\x18\x02 \x01(\x0e2\x1d.neptune.ingest.v1.IngestCode\x12\x0f\n\x07message\x18\x03 \x01(\t\x12\r\n\x05field\x18\x04 \x01(\t"\x8a\x01\n\x10UpdateRunResults\x12<\n\x12operations_summary\x18\x01 \x01(\x0b2 .neptune.ingest.v1.ResultSummary\x128\n\rupdate_errors\x18\x02 \x03(\x0b2!.neptune.ingest.v1.UpdateRunError"\xb6\x03\n\x0bBatchResult\x12#\n\tgrpc_code\x18\x01 \x01(\x0e2\x10.google_rpc.Code\x122\n\x0bingest_code\x18\t \x01(\x0e2\x1d.neptune.ingest.v1.IngestCode\x12<\n\x12operations_summary\x18\x02 \x01(\x0b2 .neptune.ingest.v1.ResultSummary\x12\x0f\n\x07project\x18\x05 \x01(\t\x12>\n\x12create_run_results\x18\x06 \x03(\x0b2".neptune.ingest.v1.CreateRunResult\x12P\n\x12update_run_results\x18\x07 \x03(\x0b24.neptune.ingest.v1.BatchResult.UpdateRunResultsEntry\x12\x0f\n\x07message\x18\x08 \x01(\t\x1a\\\n\x15UpdateRunResultsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x122\n\x05value\x18\x02 \x01(\x0b2#.neptune.ingest.v1.UpdateRunResults:\x028\x01"K\n\rIngestRequest\x12:\n\x07batches\x18\x01 \x03(\x0b2).neptune.ingest.v1.BatchProjectOperations"\x91\x01\n\x0eIngestResponse\x121\n\x07summary\x18\x02 \x01(\x0b2 .neptune.ingest.v1.ResultSummary\x12\x15\n\rerror_message\x18\x04 \x01(\t\x125\n\rbatch_results\x18\x05 \x03(\x0b2\x1e.neptune.ingest.v1.BatchResult*\xe8\x07\n\nIngestCode\x12\x06\n\x02OK\x10\x00\x12!\n\x1dBATCH_CONTAINS_DEPENDENT_RUNS\x10\x04\x12\x15\n\x11PROJECT_NOT_FOUND\x10\x08\x12\x18\n\x14PROJECT_INVALID_NAME\x10\t\x12\x11\n\rRUN_NOT_FOUND\x10\n\x12\x11\n\rRUN_DUPLICATE\x10\x0b\x12\x13\n\x0fRUN_CONFLICTING\x10\x0c\x12\x1d\n\x19RUN_FORK_PARENT_NOT_FOUND\x10\r\x12#\n\x1fRUN_INVALID_CREATION_PARAMETERS\x10\x0e\x12!\n\x1dFIELD_PATH_EXCEEDS_SIZE_LIMIT\x10\x10\x12\x14\n\x10FIELD_PATH_EMPTY\x10\x11\x12\x16\n\x12FIELD_PATH_INVALID\x10\x12\x12\x1b\n\x17FIELD_PATH_NON_WRITABLE\x10\x13\x12\x1a\n\x16FIELD_TYPE_UNSUPPORTED\x10\x14\x12\x1a\n\x16FIELD_TYPE_CONFLICTING\x10\x15\x12\x1a\n\x16SERIES_POINT_DUPLICATE\x10\x18\x12\x19\n\x15SERIES_STEP_TOO_LARGE\x10(\x12\x17\n\x13SERIES_STEP_INVALID\x10)\x125\n1SERIES_PREVIEW_STEP_NOT_AFTER_LAST_COMMITTED_STEP\x10*\x12\x1e\n\x1aSERIES_STEP_NON_INCREASING\x10\x19\x12$\n SERIES_STEP_NOT_AFTER_FORK_POINT\x10\x1a\x12\x1f\n\x1bSERIES_TIMESTAMP_DECREASING\x10\x1b\x12#\n\x1fFLOAT_VALUE_NAN_INF_UNSUPPORTED\x10 \x12\x1f\n\x1bDATETIME_VALUE_OUT_OF_RANGE\x10!\x12#\n\x1fSTRING_VALUE_EXCEEDS_SIZE_LIMIT\x10$\x12!\n\x1dSTRING_SET_EXCEEDS_SIZE_LIMIT\x10%\x12\x1f\n\x1bFILE_REF_EXCEEDS_SIZE_LIMIT\x10&\x12$\n HISTOGRAM_BIN_EDGES_CONTAINS_NAN\x102\x12\x1b\n\x17HISTOGRAM_TOO_MANY_BINS\x103\x12&\n"HISTOGRAM_BIN_EDGES_NOT_INCREASING\x104\x12-\n)HISTOGRAM_VALUES_LENGTH_DOESNT_MATCH_BINS\x105\x12\x14\n\x10INGEST_SUSPENDED\x10?\x12\x0c\n\x08INTERNAL\x10@B5\n$ml.neptune.leaderboard.api.ingest.v1B\x0bIngestProtoP\x01b\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'neptune_pb.ingest.v1.ingest_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
    _globals['DESCRIPTOR']._options = None
    _globals['DESCRIPTOR']._serialized_options = b'\n$ml.neptune.leaderboard.api.ingest.v1B\x0bIngestProtoP\x01'
    _globals['_BATCHPROJECTOPERATIONS_UPDATERUNSENTRY']._options = None
    _globals['_BATCHPROJECTOPERATIONS_UPDATERUNSENTRY']._serialized_options = b'8\x01'
    _globals['_BATCHRESULT_UPDATERUNRESULTSENTRY']._options = None
    _globals['_BATCHRESULT_UPDATERUNRESULTSENTRY']._serialized_options = b'8\x01'
    _globals['_INGESTCODE']._serialized_start = 2196
    _globals['_INGESTCODE']._serialized_end = 3196
    _globals['_BATCHCONTEXT']._serialized_start = 114
    _globals['_BATCHCONTEXT']._serialized_end = 162
    _globals['_UPDATERUN']._serialized_start = 164
    _globals['_UPDATERUN']._serialized_end = 250
    _globals['_BATCHPROJECTOPERATIONS']._serialized_start = 253
    _globals['_BATCHPROJECTOPERATIONS']._serialized_end = 565
    _globals['_BATCHPROJECTOPERATIONS_UPDATERUNSENTRY']._serialized_start = 486
    _globals['_BATCHPROJECTOPERATIONS_UPDATERUNSENTRY']._serialized_end = 565
    _globals['_CREATERUNERROR']._serialized_start = 567
    _globals['_CREATERUNERROR']._serialized_end = 689
    _globals['_CREATERUNRESULT']._serialized_start = 691
    _globals['_CREATERUNRESULT']._serialized_end = 807
    _globals['_RESULTSUMMARY']._serialized_start = 810
    _globals['_RESULTSUMMARY']._serialized_end = 1246
    _globals['_RESULTSUMMARY_FAILEDBYGRPCCODECOUNTERS']._serialized_start = 1079
    _globals['_RESULTSUMMARY_FAILEDBYGRPCCODECOUNTERS']._serialized_end = 1157
    _globals['_RESULTSUMMARY_INGESTCODECOUNTERS']._serialized_start = 1159
    _globals['_RESULTSUMMARY_INGESTCODECOUNTERS']._serialized_end = 1246
    _globals['_UPDATERUNERROR']._serialized_start = 1249
    _globals['_UPDATERUNERROR']._serialized_end = 1386
    _globals['_UPDATERUNRESULTS']._serialized_start = 1389
    _globals['_UPDATERUNRESULTS']._serialized_end = 1527
    _globals['_BATCHRESULT']._serialized_start = 1530
    _globals['_BATCHRESULT']._serialized_end = 1968
    _globals['_BATCHRESULT_UPDATERUNRESULTSENTRY']._serialized_start = 1876
    _globals['_BATCHRESULT_UPDATERUNRESULTSENTRY']._serialized_end = 1968
    _globals['_INGESTREQUEST']._serialized_start = 1970
    _globals['_INGESTREQUEST']._serialized_end = 2045
    _globals['_INGESTRESPONSE']._serialized_start = 2048
    _globals['_INGESTRESPONSE']._serialized_end = 2193