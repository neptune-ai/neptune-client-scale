from __future__ import annotations

from dataclasses import dataclass

from neptune_scale.sync.parameters import (
    MAX_SINGLE_OPERATION_SIZE_BYTES,
    MAX_STRING_SERIES_DATA_POINT_LENGTH,
)

__all__ = ("MetadataSplitter", "datetime_to_proto", "make_step", "Metrics", "StringSeries")

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
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

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
        string_series: Optional[StringSeries],
        add_tags: Optional[dict[str, Union[list[str], set[str], tuple[str]]]],
        remove_tags: Optional[dict[str, Union[list[str], set[str], tuple[str]]]],
        max_message_bytes_size: int = MAX_SINGLE_OPERATION_SIZE_BYTES,
    ):
        if metrics and string_series:
            assert metrics.step == string_series.step

        self._timestamp = datetime_to_proto(timestamp)
        self._project = project
        self._run_id = run_id

        if metrics is not None and metrics.step is not None:
            self._step = make_step(number=metrics.step)
        elif string_series is not None and string_series.step is not None:
            self._step = make_step(number=string_series.step)
        else:
            self._step = None
        self._metrics = peekable(self._stream_metrics(metrics.step, metrics.data)) if metrics is not None else None
        self._metrics_preview = metrics.preview if metrics is not None else False
        self._metrics_preview_completion = metrics.preview_completion if metrics is not None else 0.0
        self._string_series = peekable(self._stream_string_series(string_series.data)) if string_series else None

        self._configs = peekable(self._stream_configs(configs)) if configs else None
        self._add_tags = peekable(self._stream_tags(add_tags)) if add_tags else None
        self._remove_tags = peekable(self._stream_tags(remove_tags)) if remove_tags else None

        self._max_update_bytes_size = (
            max_message_bytes_size
            - RunOperation(
                project=self._project,
                run_id=self._run_id,
            ).ByteSize()
        )
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

        update = self._make_empty_update_snapshot()
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
        size = self.populate_append_string_series(
            update=update,
            assets=self._string_series,
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

    def populate_append_string_series(
        self,
        update: UpdateRunSnapshot,
        assets: Optional[peekable[tuple[str, str, int]]],
        size: int,
    ) -> int:
        if assets is None:
            return size

        while size < self._max_update_bytes_size:
            try:
                key, value, value_size = assets.peek()
            except StopIteration:
                break

            new_size = size + proto_string_size(key) + value_size + 6
            if new_size > self._max_update_bytes_size:
                break

            update.append[key].string = value
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

    def _validate_paths(self, fields: dict[str, T]) -> Iterator[tuple[str, T]]:
        _is_instance = isinstance  # local binding, faster in tight loops
        for key, value in fields.items():
            if not _is_instance(key, str):
                _warn_or_raise_on_invalid_value(f"Field paths must be strings (got `{key}`)")
                continue

            yield key, value

    def _stream_metrics(self, step: Optional[float | int], metrics: dict[str, float]) -> Iterator[tuple[str, float]]:
        for key, value in self._validate_paths(metrics):
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

    def _stream_string_series(self, string_series: dict[str, str]) -> Iterator[tuple[str, str, int]]:
        _is_instance = isinstance  # local binding, faster in tight loops
        for key, value in self._validate_paths(string_series):
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

            # Use proto_bytes_size to avoid encoding the value multiple times for size calculation
            yield key, value, proto_bytes_size(data)

    def _stream_configs(
        self, configs: dict[str, Union[float, bool, int, str, datetime, list, set, tuple]]
    ) -> Iterator[tuple[str, Value]]:
        _is_instance = isinstance  # local binding, faster in tight loops
        for key, value in self._validate_paths(configs):
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
        for key, values in self._validate_paths(tags):
            if not _is_instance(values, accepted_tag_collection_types) or any(
                not _is_instance(tag, str) for tag in values
            ):
                _warn_or_raise_on_invalid_value(
                    f"Tags must be a list, set or tuple of strings (got `{key}`:`{values}`)"
                )
                continue

            yield key, values

    def _make_empty_update_snapshot(self) -> UpdateRunSnapshot:
        include_preview = self._metrics and self._metrics_preview
        return UpdateRunSnapshot(
            step=self._step,
            preview=(self._make_preview() if include_preview else None),
            timestamp=self._timestamp,
            assign={},
            append={},
            modify_sets={},
        )

    def _make_preview(self) -> Optional[Preview]:
        if not self._metrics_preview:
            return None
        # let backend default completion
        if self._metrics_preview_completion is not None:
            return Preview(is_preview=True, completion_ratio=self._metrics_preview_completion)
        return Preview(is_preview=True)


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
    Calculates the size of the string in the protobuf message including an overhead of the length prefix (varint).
    """

    return proto_bytes_size(bytes(string, "utf-8"))


def proto_bytes_size(data: bytes) -> int:
    """
    Calculate the size of the bytes buffer encoded in a protobuf message.

    This assumes that the field tag is lower than 2048. This condition
    is true for proto fields that we are interested in (RunOperation and Value in particular).

    See inline comments for more details
    """

    # See https://protobuf.dev/programming-guides/encoding/#structure for details on encoding.
    # In short, fields are encoded as [tag][length][data bytes]

    # Protobuf uses "varint encoding" for integers in which each byte can hold 7 bits of integer data.
    # In order to determine how many bytes an integer needs, we get modulo of data_size.big_length() and 7,
    # and add 1 byte if there is a remainder, to fit the remaining bits.
    data_size = len(data)
    full, rem = divmod(data_size.bit_length(), 7)
    length_size = full + (1 if rem else 0)

    # Tag holds both the field type and the field number encoded as varint.
    # Tag is always at least 1 byte, of which 3 bits are used for data type,
    # and 4 bits are used for the field number, which gives us 2**4 = 16 possible field numbers.
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
