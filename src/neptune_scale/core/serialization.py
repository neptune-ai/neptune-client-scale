from __future__ import annotations

__all__ = (
    "mod_tags",
    "make_value",
    "make_step",
    "datetime_to_proto",
    "pb_key_size",
)

from datetime import datetime

from google.protobuf.timestamp_pb2 import Timestamp
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
    SET_OPERATION,
    ModifySet,
    Step,
    StringSet,
    Value,
)


def mod_tags(add: list[str] | set[str] | None = None, remove: list[str] | set[str] | None = None) -> ModifySet:
    mod_set = ModifySet()
    if add is not None:
        for tag in add:
            mod_set.string.values[tag] = SET_OPERATION.ADD
    if remove is not None:
        for tag in remove:
            mod_set.string.values[tag] = SET_OPERATION.REMOVE
    return mod_set


def make_value(value: Value | float | str | int | bool | datetime | list[str] | set[str]) -> Value:
    if isinstance(value, Value):
        return value
    if isinstance(value, float):
        return Value(float64=value)
    elif isinstance(value, bool):
        return Value(bool=value)
    elif isinstance(value, int):
        return Value(int64=value)
    elif isinstance(value, str):
        return Value(string=value)
    elif isinstance(value, datetime):
        return Value(timestamp=datetime_to_proto(value))
    elif isinstance(value, (list, set)):
        fv = Value(string_set=StringSet(values=value))
        return fv
    else:
        raise ValueError(f"Unsupported ingest field value type: {type(value)}")


def datetime_to_proto(dt: datetime) -> Timestamp:
    dt_ts = dt.timestamp()
    return Timestamp(seconds=int(dt_ts), nanos=int((dt_ts % 1) * 1e9))


def make_step(number: float | int, raise_on_step_precision_loss: bool = False) -> Step:
    """
    Converts a number to protobuf Step value. Example:
    >>> assert make_step(7.654321, True) == Step(whole=7, micro=654321)
    Args:
        number: step expressed as number
        raise_on_step_precision_loss: inform converter whether it should silently drop precision and
            round down to 6 decimal places or raise an error.

    Returns: Step protobuf used in Neptune API.
    """
    m = int(1e6)
    micro: int = int(number * m)
    if raise_on_step_precision_loss and number * m - micro != 0:
        raise ValueError(f"step must not use more than 6-decimal points, got: {number}")

    whole = micro // m
    micro = micro % m

    return Step(whole=whole, micro=micro)


def pb_key_size(key: str) -> int:
    key_bin = bytes(key, "utf-8")
    return len(key_bin) + 2 + (1 if len(key_bin) > 127 else 0)