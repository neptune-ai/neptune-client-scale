from __future__ import annotations

from dataclasses import dataclass

from neptune_scale.sync.parameters import (
    MAX_SINGLE_OPERATION_SIZE_BYTES,
    MAX_STRING_SERIES_DATA_POINT_LENGTH,
)

__all__ = (
    "MetadataSplitter",
    "datetime_to_proto",
    "make_step",
    "Metrics",
    "StringSeries",
    "string_series_to_update_run_snapshots",
    "proto_encoded_bytes_field_size",
)

import math
import warnings
from collections.abc import Iterator
from datetime import datetime
from typing import (
    Any,
    Optional,
    TypeVar,
    Union,
)

from google.protobuf.timestamp_pb2 import Timestamp
from more_itertools import peekable
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
    SET_OPERATION,
    Preview,
    Step,
    StringSet,
    UpdateRunSnapshot,
    Value,
)

from neptune_scale.exceptions import (
    NeptuneFloatValueNanInfUnsupported,
    NeptuneScaleWarning,
    NeptuneUnableToLogData,
)
from neptune_scale.util import (
    envs,
    get_logger,
)

logger = get_logger()

T = TypeVar("T")

SINGLE_FLOAT_VALUE_SIZE = Value(float64=1.0).ByteSize()


INVALID_VALUE_ACTION = envs.get_option(envs.LOG_FAILURE_ACTION, ("drop", "raise"), "drop")
SHOULD_SKIP_NON_FINITE_METRICS = envs.get_bool(envs.SKIP_NON_FINITE_METRICS, True)


@dataclass
class Metrics:
    """Class representing a set of metrics at a single step"""

    data: dict[str, Union[float, int]]
    step: Optional[Union[float, int]]
    preview: bool = False
    preview_completion: Optional[float] = None


@dataclass(frozen=True)
class StringSeries:
    data: dict[str, str]
    step: Union[float, int]


class MetadataSplitter(Iterator[UpdateRunSnapshot]):
    def __init__(
        self,
        *,
        project: str,
        run_id: str,
        timestamp: datetime,
        configs: Optional[dict[str, Union[float, bool, int, str, datetime, list, set, tuple]]],
        metrics: Optional[Metrics],
        add_tags: Optional[dict[str, Union[list[str], set[str], tuple[str]]]],
        remove_tags: Optional[dict[str, Union[list[str], set[str], tuple[str]]]],
        max_message_bytes_size: int = MAX_SINGLE_OPERATION_SIZE_BYTES,
    ):
        self._timestamp = datetime_to_proto(timestamp)
        self._project = project
        self._run_id = run_id

        self._metrics = peekable(self._stream_metrics(metrics.step, metrics.data)) if metrics is not None else None
        self._step = make_step(number=metrics.step) if (metrics is not None and metrics.step is not None) else None
        self._preview = _make_preview_from_metrics(metrics) if metrics else None

        self._configs = peekable(self._stream_configs(configs)) if configs else None
        self._add_tags = peekable(self._stream_tags(add_tags)) if add_tags else None
        self._remove_tags = peekable(self._stream_tags(remove_tags)) if remove_tags else None

        self._max_update_bytes_size = max_message_bytes_size
        self._has_returned = False

    def __iter__(self) -> MetadataSplitter:
        self._has_returned = False
        return self

    def __next__(self) -> UpdateRunSnapshot:
        if (
            self._has_returned
            and not self._configs
            and not self._metrics
            and not self._add_tags
            and not self._remove_tags
        ):
            raise StopIteration

        update = UpdateRunSnapshot(step=self._step, timestamp=self._timestamp, preview=self._preview)
        size = update.ByteSize()

        size = self.populate_assign(
            update=update,
            assets=self._configs,
            size=size,
        )
        size = self.populate_append_metrics(
            update=update,
            assets=self._metrics,
            size=size,
        )
        size = self.populate_tags(
            update=update,
            assets=self._add_tags,
            operation=SET_OPERATION.ADD,
            size=size,
        )
        size = self.populate_tags(
            update=update,
            assets=self._remove_tags,
            operation=SET_OPERATION.REMOVE,
            size=size,
        )

        self._has_returned = True
        return update

    def populate_assign(
        self,
        update: UpdateRunSnapshot,
        assets: Optional[peekable[tuple[str, Value]]],
        size: int,
    ) -> int:
        if assets is None:
            return size

        while size < self._max_update_bytes_size:
            try:
                key, value = assets.peek()
            except StopIteration:
                break

            new_size = size + proto_string_size(key) + value.ByteSize() + 6
            if new_size > self._max_update_bytes_size:
                break

            update.assign[key].MergeFrom(value)
            size, _ = new_size, next(assets)

        return size

    def populate_append_metrics(
        self,
        update: UpdateRunSnapshot,
        assets: Optional[peekable[tuple[str, float]]],
        size: int,
    ) -> int:
        if assets is None:
            return size

        while size < self._max_update_bytes_size:
            try:
                key, value = assets.peek()
            except StopIteration:
                break

            new_size = size + proto_string_size(key) + SINGLE_FLOAT_VALUE_SIZE + 6
            if new_size > self._max_update_bytes_size:
                break

            update.append[key].float64 = value
            size, _ = new_size, next(assets)

        return size

    def populate_tags(
        self, update: UpdateRunSnapshot, assets: Optional[peekable[Any]], operation: SET_OPERATION.ValueType, size: int
    ) -> int:
        if assets is None:
            return size

        while size < self._max_update_bytes_size:
            try:
                key, values = assets.peek()
            except StopIteration:
                break

            if not isinstance(values, peekable):
                values = peekable(values)

            is_full = False
            new_size = size + proto_string_size(key) + 6
            for value in values:
                tag_size = proto_string_size(value) + 6
                if new_size + tag_size > self._max_update_bytes_size:
                    values.prepend(value)
                    is_full = True
                    break

                update.modify_sets[key].string.values[value] = operation
                new_size += tag_size

            size, _ = new_size, next(assets)

            if is_full:
                assets.prepend((key, list(values)))
                break

        return size

    def _stream_metrics(self, step: Optional[float | int], metrics: dict[str, float]) -> Iterator[tuple[str, float]]:
        for key, value in _validate_paths(metrics):
            try:
                value = float(value)
            except (ValueError, TypeError, OverflowError):
                _warn_or_raise_on_invalid_value(f"Metrics' values must be float or int (got `{key}`:`{value}`)")
                continue

            if not math.isfinite(value):
                if SHOULD_SKIP_NON_FINITE_METRICS:
                    warnings.warn(
                        f"Neptune is skipping non-finite metric values. You can turn this warning into an error by "
                        f"setting the `{envs.SKIP_NON_FINITE_METRICS}` environment variable to `False`.",
                        category=NeptuneScaleWarning,
                        stacklevel=7,
                    )

                    logger.warning(f"Skipping a non-finite value `{value}` of metric `{key}` at step `{step}`. ")
                    continue
                else:
                    raise NeptuneFloatValueNanInfUnsupported(metric=key, step=step, value=value)

            yield key, value

    def _stream_configs(
        self, configs: dict[str, Union[float, bool, int, str, datetime, list, set, tuple]]
    ) -> Iterator[tuple[str, Value]]:
        _is_instance = isinstance  # local binding, faster in tight loops
        for key, value in _validate_paths(configs):
            if _is_instance(value, float):
                yield key, Value(float64=value)
            elif _is_instance(value, bool):
                yield key, Value(bool=value)
            elif _is_instance(value, int):
                yield key, Value(int64=value)
            elif _is_instance(value, str):
                yield key, Value(string=value)
            elif _is_instance(value, datetime):
                yield key, Value(timestamp=datetime_to_proto(value))  # type: ignore
            elif _is_instance(value, (list, set, tuple)):
                yield key, Value(string_set=StringSet(values=value))
            else:
                _warn_or_raise_on_invalid_value(
                    f"Config values must be float, bool, int, str, datetime, list, set or tuple "
                    f"(got `{key}`:`{value}`)"
                )
                continue

    def _stream_tags(
        self, tags: dict[str, Union[list[str], set[str], tuple[str]]]
    ) -> Iterator[tuple[str, Union[list[str], set[str], tuple[str]]]]:
        accepted_tag_collection_types = (list, set, tuple)
        _is_instance = isinstance  # local binding, faster in tight loops
        for key, values in _validate_paths(tags):
            if not _is_instance(values, accepted_tag_collection_types) or any(
                not _is_instance(tag, str) for tag in values
            ):
                _warn_or_raise_on_invalid_value(
                    f"Tags must be a list, set or tuple of strings (got `{key}`:`{values}`)"
                )
                continue

            yield key, values


def _validate_paths(fields: dict[str, T]) -> Iterator[tuple[str, T]]:
    _is_instance = isinstance  # local binding, faster in tight loops
    for key, value in fields.items():
        if not _is_instance(key, str):
            _warn_or_raise_on_invalid_value(f"Field paths must be strings (got `{key}`)")
            continue

        yield key, value


def _make_preview_from_metrics(metrics: Metrics) -> Optional[Preview]:
    if not metrics.preview:
        return None
    # let backend default completion
    if metrics.preview_completion is not None:
        return Preview(is_preview=True, completion_ratio=metrics.preview_completion)
    return Preview(is_preview=True)


def string_series_to_update_run_snapshots(
    string_series: Optional[StringSeries],
    timestamp: datetime,
    max_size: int = MAX_SINGLE_OPERATION_SIZE_BYTES,
) -> Iterator[UpdateRunSnapshot]:
    if not string_series:
        return

    stream = peekable(_stream_string_series(string_series.data))
    step = make_step(string_series.step)
    timestamp = datetime_to_proto(timestamp)

    # Local bindings for faster name lookups
    _proto_string_size = proto_string_size
    _peek_stream = stream.peek
    while stream:
        update = UpdateRunSnapshot(step=step, timestamp=timestamp)

        size = 0
        while size < max_size:
            try:
                key, value, value_size = _peek_stream()
            except StopIteration:
                break

            new_size = size + _proto_string_size(key) + value_size + 6
            if new_size > max_size:
                break

            update.append[key].string = value
            size, _ = new_size, next(stream)

        yield update


def _stream_string_series(string_series: dict[str, str]) -> Iterator[tuple[str, str, int]]:
    _is_instance = isinstance  # local binding, faster in tight loops
    for key, value in _validate_paths(string_series):
        if not isinstance(value, str):
            _warn_or_raise_on_invalid_value(f"String series values must be strings (got `{key}`:`{value}`)")
            continue

        data = value.encode("utf-8")
        if len(data) > MAX_STRING_SERIES_DATA_POINT_LENGTH:
            _warn_or_raise_on_invalid_value(
                f"String series values must be less than {MAX_STRING_SERIES_DATA_POINT_LENGTH}"
                " bytes when UTF-8 encoded"
            )
            continue

        # Pass the value along with its proto_bytes_size to avoid encoding multiple times for size calculation
        yield key, value, proto_bytes_size(data)


def datetime_to_proto(dt: datetime) -> Timestamp:
    dt_ts = dt.timestamp()
    return Timestamp(seconds=int(dt_ts), nanos=int((dt_ts % 1) * 1e9))


def make_step(number: Union[float, int], raise_on_step_precision_loss: bool = False) -> Step:
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


def _warn_or_raise_on_invalid_value(message: str) -> None:
    if INVALID_VALUE_ACTION == "drop":
        logger.warning(f"Dropping value. {message}.")
    else:
        raise NeptuneUnableToLogData(message)
