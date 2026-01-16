"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_sym_db = _symbol_database.Default()
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n!neptune_pb/ingest/v1/common.proto\x12\x11neptune.ingest.v1\x1a\x1fgoogle/protobuf/timestamp.proto"$\n\x04Step\x12\r\n\x05whole\x18\x01 \x01(\x04\x12\r\n\x05micro\x18\x02 \x01(\x04"\xb3\x01\n\tForkPoint\x12\x1b\n\x0eparent_project\x18\x01 \x01(\tH\x00\x88\x01\x01\x12\x15\n\rparent_run_id\x18\x02 \x01(\t\x12%\n\x04step\x18\x04 \x01(\x0b2\x17.neptune.ingest.v1.Step\x12 \n\x13requested_parent_id\x18\x0f \x01(\tH\x01\x88\x01\x01B\x11\n\x0f_parent_projectB\x16\n\x14_requested_parent_id"\x1b\n\tStringSet\x12\x0e\n\x06values\x18\x01 \x03(\t">\n\x07FileRef\x12\x0c\n\x04path\x18\x01 \x01(\t\x12\x12\n\nsize_bytes\x18\x02 \x01(\x03\x12\x11\n\tmime_type\x18\x03 \x01(\t"\xd3\x01\n\tHistogram\x12\x11\n\tbin_edges\x18\x01 \x03(\x01\x125\n\x06counts\x18\x04 \x01(\x0b2#.neptune.ingest.v1.Histogram.CountsH\x00\x12;\n\tdensities\x18\x05 \x01(\x0b2&.neptune.ingest.v1.Histogram.DensitiesH\x00\x1a\x18\n\x06Counts\x12\x0e\n\x06values\x18\x01 \x03(\x04\x1a\x1b\n\tDensities\x12\x0e\n\x06values\x18\x01 \x03(\x01B\x08\n\x06values"\x9e\x02\n\x05Value\x12\x11\n\x07float64\x18\x01 \x01(\x01H\x00\x12\x0f\n\x05int64\x18\x03 \x01(\x03H\x00\x12\x0e\n\x04bool\x18\x05 \x01(\x08H\x00\x12\x10\n\x06string\x18\x06 \x01(\tH\x00\x12/\n\ttimestamp\x18\x08 \x01(\x0b2\x1a.google.protobuf.TimestampH\x00\x122\n\nstring_set\x18\x0c \x01(\x0b2\x1c.neptune.ingest.v1.StringSetH\x00\x12.\n\x08file_ref\x18\r \x01(\x0b2\x1a.neptune.ingest.v1.FileRefH\x00\x121\n\thistogram\x18\x0e \x01(\x0b2\x1c.neptune.ingest.v1.HistogramH\x00B\x07\n\x05value"\xa2\x01\n\x0fModifyStringSet\x12>\n\x06values\x18\x01 \x03(\x0b2..neptune.ingest.v1.ModifyStringSet.ValuesEntry\x1aO\n\x0bValuesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12/\n\x05value\x18\x02 \x01(\x0e2 .neptune.ingest.v1.SET_OPERATION:\x028\x01"I\n\tModifySet\x124\n\x06string\x18\x01 \x01(\x0b2".neptune.ingest.v1.ModifyStringSetH\x00B\x06\n\x04type"F\n\x05Owner\x12\x11\n\x07user_id\x18\x01 \x01(\tH\x00\x12\x1c\n\x12service_account_id\x18\x02 \x01(\tH\x00B\x0c\n\nowner_type"p\n\x0eOffsetMetadata\x12\x1b\n\x0ebroker_version\x18\x01 \x01(\rH\x00\x88\x01\x01\x12\x1b\n\x0emessage_offset\x18\x02 \x01(\x04H\x01\x88\x01\x01B\x11\n\x0f_broker_versionB\x11\n\x0f_message_offset"\xe6\x04\n\x03Run\x12\x13\n\x06run_id\x18\x01 \x01(\tH\x00\x88\x01\x01\x12\x1a\n\rexperiment_id\x18\x05 \x01(\tH\x01\x88\x01\x01\x125\n\nfork_point\x18\x02 \x01(\x0b2\x1c.neptune.ingest.v1.ForkPointH\x02\x88\x01\x01\x12\x13\n\x06family\x18\x04 \x01(\tH\x03\x88\x01\x01\x126\n\rcreation_time\x18\x03 \x01(\x0b2\x1a.google.protobuf.TimestampH\x04\x88\x01\x01\x12,\n\x05owner\x18\x06 \x01(\x0b2\x18.neptune.ingest.v1.OwnerH\x05\x88\x01\x01\x12\x17\n\nrequest_id\x18\x07 \x01(\tH\x06\x88\x01\x01\x12\x14\n\x07tracker\x18\x08 \x01(\tH\x07\x88\x01\x01\x12\x1c\n\x0finherit_configs\x18\t \x01(\x08H\x08\x88\x01\x01\x12>\n\x15start_processing_time\x18\x0b \x01(\x0b2\x1a.google.protobuf.TimestampH\t\x88\x01\x01\x12?\n\x0foffset_metadata\x18\x0c \x01(\x0b2!.neptune.ingest.v1.OffsetMetadataH\n\x88\x01\x01B\t\n\x07_run_idB\x10\n\x0e_experiment_idB\r\n\x0b_fork_pointB\t\n\x07_familyB\x10\n\x0e_creation_timeB\x08\n\x06_ownerB\r\n\x0b_request_idB\n\n\x08_trackerB\x12\n\x10_inherit_configsB\x18\n\x16_start_processing_timeB\x12\n\x10_offset_metadata"7\n\x07Preview\x12\x12\n\nis_preview\x18\x01 \x01(\x08\x12\x18\n\x10completion_ratio\x18\x02 \x01(\x02"\xd1\x06\n\x11UpdateRunSnapshot\x12*\n\x04step\x18\x01 \x01(\x0b2\x17.neptune.ingest.v1.StepH\x00\x88\x01\x01\x122\n\ttimestamp\x18\x02 \x01(\x0b2\x1a.google.protobuf.TimestampH\x01\x88\x01\x01\x120\n\x07preview\x18\n \x01(\x0b2\x1a.neptune.ingest.v1.PreviewH\x02\x88\x01\x01\x12@\n\x06assign\x18\x04 \x03(\x0b20.neptune.ingest.v1.UpdateRunSnapshot.AssignEntry\x12I\n\x0bmodify_sets\x18\x05 \x03(\x0b24.neptune.ingest.v1.UpdateRunSnapshot.ModifySetsEntry\x12@\n\x06append\x18\x08 \x03(\x0b20.neptune.ingest.v1.UpdateRunSnapshot.AppendEntry\x12\x17\n\nrequest_id\x18\t \x01(\tH\x03\x88\x01\x01\x12>\n\x15start_processing_time\x18\x0b \x01(\x0b2\x1a.google.protobuf.TimestampH\x04\x88\x01\x01\x12?\n\x0foffset_metadata\x18\x0c \x01(\x0b2!.neptune.ingest.v1.OffsetMetadataH\x05\x88\x01\x01\x1aG\n\x0bAssignEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\'\n\x05value\x18\x02 \x01(\x0b2\x18.neptune.ingest.v1.Value:\x028\x01\x1aO\n\x0fModifySetsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12+\n\x05value\x18\x02 \x01(\x0b2\x1c.neptune.ingest.v1.ModifySet:\x028\x01\x1aG\n\x0bAppendEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\'\n\x05value\x18\x02 \x01(\x0b2\x18.neptune.ingest.v1.Value:\x028\x01B\x07\n\x05_stepB\x0c\n\n_timestampB\n\n\x08_previewB\r\n\x0b_request_idB\x18\n\x16_start_processing_timeB\x12\n\x10_offset_metadata"M\n\x12UpdateRunSnapshots\x127\n\tsnapshots\x18\x01 \x03(\x0b2$.neptune.ingest.v1.UpdateRunSnapshot*.\n\rSET_OPERATION\x12\x08\n\x04NOOP\x10\x00\x12\x07\n\x03ADD\x10\x01\x12\n\n\x06REMOVE\x10\x02B5\n$ml.neptune.leaderboard.api.ingest.v1B\x0bCommonProtoP\x01b\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'neptune_pb.ingest.v1.common_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
    _globals['DESCRIPTOR']._options = None
    _globals['DESCRIPTOR']._serialized_options = b'\n$ml.neptune.leaderboard.api.ingest.v1B\x0bCommonProtoP\x01'
    _globals['_MODIFYSTRINGSET_VALUESENTRY']._options = None
    _globals['_MODIFYSTRINGSET_VALUESENTRY']._serialized_options = b'8\x01'
    _globals['_UPDATERUNSNAPSHOT_ASSIGNENTRY']._options = None
    _globals['_UPDATERUNSNAPSHOT_ASSIGNENTRY']._serialized_options = b'8\x01'
    _globals['_UPDATERUNSNAPSHOT_MODIFYSETSENTRY']._options = None
    _globals['_UPDATERUNSNAPSHOT_MODIFYSETSENTRY']._serialized_options = b'8\x01'
    _globals['_UPDATERUNSNAPSHOT_APPENDENTRY']._options = None
    _globals['_UPDATERUNSNAPSHOT_APPENDENTRY']._serialized_options = b'8\x01'
    _globals['_SET_OPERATION']._serialized_start = 2936
    _globals['_SET_OPERATION']._serialized_end = 2982
    _globals['_STEP']._serialized_start = 89
    _globals['_STEP']._serialized_end = 125
    _globals['_FORKPOINT']._serialized_start = 128
    _globals['_FORKPOINT']._serialized_end = 307
    _globals['_STRINGSET']._serialized_start = 309
    _globals['_STRINGSET']._serialized_end = 336
    _globals['_FILEREF']._serialized_start = 338
    _globals['_FILEREF']._serialized_end = 400
    _globals['_HISTOGRAM']._serialized_start = 403
    _globals['_HISTOGRAM']._serialized_end = 614
    _globals['_HISTOGRAM_COUNTS']._serialized_start = 551
    _globals['_HISTOGRAM_COUNTS']._serialized_end = 575
    _globals['_HISTOGRAM_DENSITIES']._serialized_start = 577
    _globals['_HISTOGRAM_DENSITIES']._serialized_end = 604
    _globals['_VALUE']._serialized_start = 617
    _globals['_VALUE']._serialized_end = 903
    _globals['_MODIFYSTRINGSET']._serialized_start = 906
    _globals['_MODIFYSTRINGSET']._serialized_end = 1068
    _globals['_MODIFYSTRINGSET_VALUESENTRY']._serialized_start = 989
    _globals['_MODIFYSTRINGSET_VALUESENTRY']._serialized_end = 1068
    _globals['_MODIFYSET']._serialized_start = 1070
    _globals['_MODIFYSET']._serialized_end = 1143
    _globals['_OWNER']._serialized_start = 1145
    _globals['_OWNER']._serialized_end = 1215
    _globals['_OFFSETMETADATA']._serialized_start = 1217
    _globals['_OFFSETMETADATA']._serialized_end = 1329
    _globals['_RUN']._serialized_start = 1332
    _globals['_RUN']._serialized_end = 1946
    _globals['_PREVIEW']._serialized_start = 1948
    _globals['_PREVIEW']._serialized_end = 2003
    _globals['_UPDATERUNSNAPSHOT']._serialized_start = 2006
    _globals['_UPDATERUNSNAPSHOT']._serialized_end = 2855
    _globals['_UPDATERUNSNAPSHOT_ASSIGNENTRY']._serialized_start = 2534
    _globals['_UPDATERUNSNAPSHOT_ASSIGNENTRY']._serialized_end = 2605
    _globals['_UPDATERUNSNAPSHOT_MODIFYSETSENTRY']._serialized_start = 2607
    _globals['_UPDATERUNSNAPSHOT_MODIFYSETSENTRY']._serialized_end = 2686
    _globals['_UPDATERUNSNAPSHOT_APPENDENTRY']._serialized_start = 2688
    _globals['_UPDATERUNSNAPSHOT_APPENDENTRY']._serialized_end = 2759
    _globals['_UPDATERUNSNAPSHOTS']._serialized_start = 2857
    _globals['_UPDATERUNSNAPSHOTS']._serialized_end = 2934