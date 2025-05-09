from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import Value

SINGLE_FLOAT_VALUE_SIZE = Value(float64=1.0).ByteSize()


def proto_string_size(string: str) -> int:
    """
    Calculate size of the string encoded in a protobuf message.

    This assumes that the field tag is lower than 2048. This condition
    is true for proto fields that we are interested in (RunOperation and Value in particular).

    See inline comments in `proto_encoded_field_size()` for more details
    """

    return proto_encoded_bytes_field_size(len(bytes(string, "utf-8")))


def proto_bytes_size(data: bytes) -> int:
    """
    Calculate size of the bytes buffer encoded in a protobuf message.

    This assumes that the field tag is lower than 2048. This condition
    is true for proto fields that we are interested in (RunOperation and Value in particular).

    See inline comments in `proto_encoded_field_size()` for more details
    """

    return proto_encoded_bytes_field_size(len(data))


def proto_encoded_bytes_field_size(data_size: int) -> int:
    """
    Calculate the total length of `data_size` bytes of data when encoded in a protobuf message.
    Returns `data_size` + <overhead>.

    The overhead is the size of the field tag and length prefix.

    This assumes that the field tag is lower than 2048. This condition
    is true for proto fields that we are interested in (RunOperation and Value in particular).

    See inline comments and https://protobuf.dev/programming-guides/encoding/#structure for more details.
    """

    # LEN-encoded fields (such as bytes and strings) are encoded as [tag][length][data bytes]
    #
    # Length is encoded as varint, an encoding in protobuf in which each byte can hold 7 bits of integer data.
    # In order to determine how many bytes an integer needs, we get modulo of data_size.big_length() and 7,
    # and add 1 byte if there is a remainder, to fit the remaining bits.
    full, rem = divmod(data_size.bit_length(), 7)
    length_size = full + (1 if rem else 0)

    # Tag holds both the field type and the field number encoded as varint.
    #
    # Tag is always at least 1 byte, of which 3 bits are used for data type,
    # and 4 bits are used for the field number, which gives us 2**4 = 16 possible field numbers.
    #
    # This means that on a single byte we can encode fields with numbers up to 15. Fields with larger
    # numbers need more space, with 7 bits for each additional byte.
    # This is why we assume 2 bytes for tag, which gives us 4 + 7 bits of data -> 2**11 = 2048 possible field numbers,
    # as the assumption for numbers lower than 15 could not hold true for all the defined message types.
    tag_size = 2

    return tag_size + length_size + data_size
