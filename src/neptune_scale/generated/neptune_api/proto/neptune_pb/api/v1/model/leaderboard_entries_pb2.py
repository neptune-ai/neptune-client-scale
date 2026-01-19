"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_sym_db = _symbol_database.Default()
from .....neptune_pb.api.v1.model import series_values_pb2 as neptune__pb_dot_api_dot_v1_dot_model_dot_series__values__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n1neptune_pb/api/v1/model/leaderboard_entries.proto\x12\x14neptune.api.v1.model\x1a+neptune_pb/api/v1/model/series_values.proto"\xb6\x01\n&ProtoLeaderboardEntriesSearchResultDTO\x12\x1b\n\x13matching_item_count\x18\x01 \x01(\x03\x12\x1e\n\x11total_group_count\x18\x02 \x01(\x03H\x00\x88\x01\x01\x129\n\x07entries\x18\x03 \x03(\x0b2(.neptune.api.v1.model.ProtoAttributesDTOB\x14\n\x12_total_group_count"\xd4\x01\n\x12ProtoAttributesDTO\x12\x15\n\rexperiment_id\x18\x01 \x01(\t\x12\x0c\n\x04type\x18\x02 \x01(\t\x12\x12\n\nproject_id\x18\x03 \x01(\t\x12\x17\n\x0forganization_id\x18\x04 \x01(\t\x12\x14\n\x0cproject_name\x18\x05 \x01(\t\x12\x19\n\x11organization_name\x18\x06 \x01(\t\x12;\n\nattributes\x18\x07 \x03(\x0b2\'.neptune.api.v1.model.ProtoAttributeDTO"\xe5\t\n\x11ProtoAttributeDTO\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x0c\n\x04type\x18\x02 \x01(\t\x12G\n\x0eint_properties\x18\x03 \x01(\x0b2*.neptune.api.v1.model.ProtoIntAttributeDTOH\x00\x88\x01\x01\x12K\n\x10float_properties\x18\x04 \x01(\x0b2,.neptune.api.v1.model.ProtoFloatAttributeDTOH\x01\x88\x01\x01\x12M\n\x11string_properties\x18\x05 \x01(\x0b2-.neptune.api.v1.model.ProtoStringAttributeDTOH\x02\x88\x01\x01\x12I\n\x0fbool_properties\x18\x06 \x01(\x0b2+.neptune.api.v1.model.ProtoBoolAttributeDTOH\x03\x88\x01\x01\x12Q\n\x13datetime_properties\x18\x07 \x01(\x0b2/.neptune.api.v1.model.ProtoDatetimeAttributeDTOH\x04\x88\x01\x01\x12T\n\x15string_set_properties\x18\x08 \x01(\x0b20.neptune.api.v1.model.ProtoStringSetAttributeDTOH\x05\x88\x01\x01\x12X\n\x17float_series_properties\x18\t \x01(\x0b22.neptune.api.v1.model.ProtoFloatSeriesAttributeDTOH\x06\x88\x01\x01\x12P\n\x13file_ref_properties\x18\n \x01(\x0b2..neptune.api.v1.model.ProtoFileRefAttributeDTOH\x07\x88\x01\x01\x12Z\n\x18string_series_properties\x18\x0b \x01(\x0b23.neptune.api.v1.model.ProtoStringSeriesAttributeDTOH\x08\x88\x01\x01\x12]\n\x1afile_ref_series_properties\x18\x0c \x01(\x0b24.neptune.api.v1.model.ProtoFileRefSeriesAttributeDTOH\t\x88\x01\x01\x12`\n\x1bhistogram_series_properties\x18\r \x01(\x0b26.neptune.api.v1.model.ProtoHistogramSeriesAttributeDTOH\n\x88\x01\x01B\x11\n\x0f_int_propertiesB\x13\n\x11_float_propertiesB\x14\n\x12_string_propertiesB\x12\n\x10_bool_propertiesB\x16\n\x14_datetime_propertiesB\x18\n\x16_string_set_propertiesB\x1a\n\x18_float_series_propertiesB\x16\n\x14_file_ref_propertiesB\x1b\n\x19_string_series_propertiesB\x1d\n\x1b_file_ref_series_propertiesB\x1e\n\x1c_histogram_series_properties"U\n\x14ProtoIntAttributeDTO\x12\x16\n\x0eattribute_name\x18\x01 \x01(\t\x12\x16\n\x0eattribute_type\x18\x02 \x01(\t\x12\r\n\x05value\x18\x03 \x01(\x03"W\n\x16ProtoFloatAttributeDTO\x12\x16\n\x0eattribute_name\x18\x01 \x01(\t\x12\x16\n\x0eattribute_type\x18\x02 \x01(\t\x12\r\n\x05value\x18\x03 \x01(\x01"X\n\x17ProtoStringAttributeDTO\x12\x16\n\x0eattribute_name\x18\x01 \x01(\t\x12\x16\n\x0eattribute_type\x18\x02 \x01(\t\x12\r\n\x05value\x18\x03 \x01(\t"V\n\x15ProtoBoolAttributeDTO\x12\x16\n\x0eattribute_name\x18\x01 \x01(\t\x12\x16\n\x0eattribute_type\x18\x02 \x01(\t\x12\r\n\x05value\x18\x03 \x01(\x08"Z\n\x19ProtoDatetimeAttributeDTO\x12\x16\n\x0eattribute_name\x18\x01 \x01(\t\x12\x16\n\x0eattribute_type\x18\x02 \x01(\t\x12\r\n\x05value\x18\x03 \x01(\x03"[\n\x1aProtoStringSetAttributeDTO\x12\x16\n\x0eattribute_name\x18\x01 \x01(\t\x12\x16\n\x0eattribute_type\x18\x02 \x01(\t\x12\r\n\x05value\x18\x03 \x03(\t"\xe9\x02\n\x1cProtoFloatSeriesAttributeDTO\x12\x16\n\x0eattribute_name\x18\x01 \x01(\t\x12\x16\n\x0eattribute_type\x18\x02 \x01(\t\x12\x16\n\tlast_step\x18\x03 \x01(\x01H\x00\x88\x01\x01\x12\x11\n\x04last\x18\x04 \x01(\x01H\x01\x88\x01\x01\x12\x10\n\x03min\x18\x05 \x01(\x01H\x02\x88\x01\x01\x12\x10\n\x03max\x18\x06 \x01(\x01H\x03\x88\x01\x01\x12\x14\n\x07average\x18\x07 \x01(\x01H\x04\x88\x01\x01\x12\x15\n\x08variance\x18\x08 \x01(\x01H\x05\x88\x01\x01\x12H\n\x06config\x18\t \x01(\x0b28.neptune.api.v1.model.ProtoFloatSeriesAttributeConfigDTO\x12\x13\n\x0bhas_preview\x18\n \x01(\x08B\x0c\n\n_last_stepB\x07\n\x05_lastB\x06\n\x04_minB\x06\n\x04_maxB\n\n\x08_averageB\x0b\n\t_variance"t\n"ProtoFloatSeriesAttributeConfigDTO\x12\x10\n\x03min\x18\x01 \x01(\x01H\x00\x88\x01\x01\x12\x10\n\x03max\x18\x02 \x01(\x01H\x01\x88\x01\x01\x12\x11\n\x04unit\x18\x03 \x01(\tH\x02\x88\x01\x01B\x06\n\x04_minB\x06\n\x04_maxB\x07\n\x05_unit"}\n\x18ProtoFileRefAttributeDTO\x12\x16\n\x0eattribute_name\x18\x01 \x01(\t\x12\x16\n\x0eattribute_type\x18\x02 \x01(\t\x12\x0c\n\x04path\x18\x03 \x01(\t\x12\x11\n\tsizeBytes\x18\x04 \x01(\x03\x12\x10\n\x08mimeType\x18\x05 \x01(\t"\x91\x01\n\x1dProtoStringSeriesAttributeDTO\x12\x16\n\x0eattribute_name\x18\x01 \x01(\t\x12\x16\n\x0eattribute_type\x18\x02 \x01(\t\x12\x16\n\tlast_step\x18\x03 \x01(\x01H\x00\x88\x01\x01\x12\x11\n\x04last\x18\x04 \x01(\tH\x01\x88\x01\x01B\x0c\n\n_last_stepB\x07\n\x05_last"\xb6\x01\n\x1eProtoFileRefSeriesAttributeDTO\x12\x16\n\x0eattribute_name\x18\x01 \x01(\t\x12\x16\n\x0eattribute_type\x18\x02 \x01(\t\x12\x16\n\tlast_step\x18\x03 \x01(\x01H\x00\x88\x01\x01\x125\n\x04last\x18\x04 \x01(\x0b2".neptune.api.v1.model.ProtoFileRefH\x01\x88\x01\x01B\x0c\n\n_last_stepB\x07\n\x05_last"\xba\x01\n ProtoHistogramSeriesAttributeDTO\x12\x16\n\x0eattribute_name\x18\x01 \x01(\t\x12\x16\n\x0eattribute_type\x18\x02 \x01(\t\x12\x16\n\tlast_step\x18\x03 \x01(\x01H\x00\x88\x01\x01\x127\n\x04last\x18\x04 \x01(\x0b2$.neptune.api.v1.model.ProtoHistogramH\x01\x88\x01\x01B\x0c\n\n_last_stepB\x07\n\x05_lastB4\n0ml.neptune.leaderboard.api.model.proto.generatedP\x01b\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'neptune_pb.api.v1.model.leaderboard_entries_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
    _globals['DESCRIPTOR']._options = None
    _globals['DESCRIPTOR']._serialized_options = b'\n0ml.neptune.leaderboard.api.model.proto.generatedP\x01'
    _globals['_PROTOLEADERBOARDENTRIESSEARCHRESULTDTO']._serialized_start = 121
    _globals['_PROTOLEADERBOARDENTRIESSEARCHRESULTDTO']._serialized_end = 303
    _globals['_PROTOATTRIBUTESDTO']._serialized_start = 306
    _globals['_PROTOATTRIBUTESDTO']._serialized_end = 518
    _globals['_PROTOATTRIBUTEDTO']._serialized_start = 521
    _globals['_PROTOATTRIBUTEDTO']._serialized_end = 1774
    _globals['_PROTOINTATTRIBUTEDTO']._serialized_start = 1776
    _globals['_PROTOINTATTRIBUTEDTO']._serialized_end = 1861
    _globals['_PROTOFLOATATTRIBUTEDTO']._serialized_start = 1863
    _globals['_PROTOFLOATATTRIBUTEDTO']._serialized_end = 1950
    _globals['_PROTOSTRINGATTRIBUTEDTO']._serialized_start = 1952
    _globals['_PROTOSTRINGATTRIBUTEDTO']._serialized_end = 2040
    _globals['_PROTOBOOLATTRIBUTEDTO']._serialized_start = 2042
    _globals['_PROTOBOOLATTRIBUTEDTO']._serialized_end = 2128
    _globals['_PROTODATETIMEATTRIBUTEDTO']._serialized_start = 2130
    _globals['_PROTODATETIMEATTRIBUTEDTO']._serialized_end = 2220
    _globals['_PROTOSTRINGSETATTRIBUTEDTO']._serialized_start = 2222
    _globals['_PROTOSTRINGSETATTRIBUTEDTO']._serialized_end = 2313
    _globals['_PROTOFLOATSERIESATTRIBUTEDTO']._serialized_start = 2316
    _globals['_PROTOFLOATSERIESATTRIBUTEDTO']._serialized_end = 2677
    _globals['_PROTOFLOATSERIESATTRIBUTECONFIGDTO']._serialized_start = 2679
    _globals['_PROTOFLOATSERIESATTRIBUTECONFIGDTO']._serialized_end = 2795
    _globals['_PROTOFILEREFATTRIBUTEDTO']._serialized_start = 2797
    _globals['_PROTOFILEREFATTRIBUTEDTO']._serialized_end = 2922
    _globals['_PROTOSTRINGSERIESATTRIBUTEDTO']._serialized_start = 2925
    _globals['_PROTOSTRINGSERIESATTRIBUTEDTO']._serialized_end = 3070
    _globals['_PROTOFILEREFSERIESATTRIBUTEDTO']._serialized_start = 3073
    _globals['_PROTOFILEREFSERIESATTRIBUTEDTO']._serialized_end = 3255
    _globals['_PROTOHISTOGRAMSERIESATTRIBUTEDTO']._serialized_start = 3258
    _globals['_PROTOHISTOGRAMSERIESATTRIBUTEDTO']._serialized_end = 3444