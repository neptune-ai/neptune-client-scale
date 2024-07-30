from __future__ import annotations

__all__ = ("datetime_to_proto",)

from datetime import datetime

from google.protobuf.timestamp_pb2 import Timestamp


def datetime_to_proto(dt: datetime) -> Timestamp:
    dt_ts = dt.timestamp()
    return Timestamp(seconds=int(dt_ts), nanos=int((dt_ts % 1) * 1e9))
