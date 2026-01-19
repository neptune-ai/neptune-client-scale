"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_sym_db = _symbol_database.Default()
from .....neptune_pb.api.v1.model import leaderboard_entries_pb2 as neptune__pb_dot_api_dot_v1_dot_model_dot_leaderboard__entries__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n(neptune_pb/api/v1/model/attributes.proto\x12\x14neptune.api.v1.model\x1a1neptune_pb/api/v1/model/leaderboard_entries.proto"d\n\x1eProtoAttributesSearchResultDTO\x12B\n\x07entries\x18\x01 \x03(\x0b21.neptune.api.v1.model.ProtoAttributeDefinitionDTO"9\n\x1bProtoAttributeDefinitionDTO\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x0c\n\x04type\x18\x02 \x01(\t"\xa9\x01\n\x1dProtoQueryAttributesResultDTO\x12N\n\x07entries\x18\x01 \x03(\x0b2=.neptune.api.v1.model.ProtoQueryAttributesExperimentResultDTO\x128\n\x08nextPage\x18\x02 \x01(\x0b2&.neptune.api.v1.model.ProtoNextPageDTO"^\n\x10ProtoNextPageDTO\x12\x1a\n\rnextPageToken\x18\x01 \x01(\tH\x00\x88\x01\x01\x12\x12\n\x05limit\x18\x02 \x01(\rH\x01\x88\x01\x01B\x10\n\x0e_nextPageTokenB\x08\n\x06_limit"\x97\x01\n\'ProtoQueryAttributesExperimentResultDTO\x12\x14\n\x0cexperimentId\x18\x01 \x01(\t\x12\x19\n\x11experimentShortId\x18\x02 \x01(\t\x12;\n\nattributes\x18\x03 \x03(\x0b2\'.neptune.api.v1.model.ProtoAttributeDTOB4\n0ml.neptune.leaderboard.api.model.proto.generatedP\x01b\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'neptune_pb.api.v1.model.attributes_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
    _globals['DESCRIPTOR']._options = None
    _globals['DESCRIPTOR']._serialized_options = b'\n0ml.neptune.leaderboard.api.model.proto.generatedP\x01'
    _globals['_PROTOATTRIBUTESSEARCHRESULTDTO']._serialized_start = 117
    _globals['_PROTOATTRIBUTESSEARCHRESULTDTO']._serialized_end = 217
    _globals['_PROTOATTRIBUTEDEFINITIONDTO']._serialized_start = 219
    _globals['_PROTOATTRIBUTEDEFINITIONDTO']._serialized_end = 276
    _globals['_PROTOQUERYATTRIBUTESRESULTDTO']._serialized_start = 279
    _globals['_PROTOQUERYATTRIBUTESRESULTDTO']._serialized_end = 448
    _globals['_PROTONEXTPAGEDTO']._serialized_start = 450
    _globals['_PROTONEXTPAGEDTO']._serialized_end = 544
    _globals['_PROTOQUERYATTRIBUTESEXPERIMENTRESULTDTO']._serialized_start = 547
    _globals['_PROTOQUERYATTRIBUTESEXPERIMENTRESULTDTO']._serialized_end = 698