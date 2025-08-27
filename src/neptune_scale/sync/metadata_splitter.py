from __future__ import annotations

import math
import warnings
from collections.abc import Iterator
from dataclasses import dataclass
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
    FileRef,
)
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import Histogram as ProtobufHistogram
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
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
from neptune_scale.sync.parameters import (
    MAX_ATTRIBUTE_PATH_LENGTH,
    MAX_FILE_DESTINATION_LENGTH,
    MAX_FILE_MIME_TYPE_LENGTH,
    MAX_HISTOGRAM_BIN_EDGES,
    MAX_SINGLE_OPERATION_SIZE_BYTES,
    MAX_STRING_SERIES_DATA_POINT_LENGTH,
)
from neptune_scale.sync.size_util import (
    SINGLE_FLOAT_VALUE_SIZE,
    proto_string_size,
)
from neptune_scale.types import Histogram
from neptune_scale.util import (
    envs,
    get_logger,
)

__all__ = (
    "FileRefData",
    "MetadataSplitter",
    "Metrics",
    "datetime_to_proto",
    "make_step",
    "histograms_to_update_run_snapshots",
    "string_series_to_update_run_snapshots",
    "sanitize_attribute_path",
)


logger = get_logger()

T = TypeVar("T")


INVALID_VALUE_ACTION = envs.get_option(envs.LOG_FAILURE_ACTION, ("drop", "raise"), "drop")
SHOULD_SKIP_NON_FINITE_METRICS = envs.get_bool(envs.SKIP_NON_FINITE_METRICS, True)


@dataclass(frozen=True)
class FileRefData:
    """Passed between Run and MetadataSplitter for file uploads"""

    # py39 does not support slots=True on @dataclass
    __slots__ = ("destination", "mime_type", "size_bytes")

    destination: str
    mime_type: str
    size_bytes: int


@dataclass
class Metrics:
    """Class representing a set of metrics at a single step"""

    data: dict[str, Union[float, int]]
    preview: bool = False
    preview_completion: Optional[float] = None


class MetadataSplitter(Iterator[UpdateRunSnapshot]):
    def __init__(
        self,
        *,
        project: str,
        run_id: str,
        timestamp: datetime,
        step: Optional[Union[float, int]],
        configs: Optional[dict[str, Union[float, bool, int, str, datetime, list, set, tuple]]],
        metrics: Optional[Metrics],
        files: Optional[dict[str, FileRefData]],
        file_series: Optional[dict[str, FileRefData]],
        add_tags: Optional[dict[str, Union[list[str], set[str], tuple[str]]]],
        remove_tags: Optional[dict[str, Union[list[str], set[str], tuple[str]]]],
        max_message_bytes_size: int = MAX_SINGLE_OPERATION_SIZE_BYTES,
    ):
        self._timestamp = datetime_to_proto(timestamp)
        self._project = project
        self._run_id = run_id

        self._step = make_step(step) if step is not None else None
        self._metrics = peekable(self._stream_metrics(step, metrics.data)) if metrics is not None else None
        self._preview = _make_preview_from_metrics(metrics) if metrics else None

        self._configs = peekable(self._stream_configs(configs)) if configs else None
        self._add_tags = peekable(self._stream_tags(add_tags)) if add_tags else None
        self._remove_tags = peekable(self._stream_tags(remove_tags)) if remove_tags else None
        self._files = peekable(self._stream_files(files)) if files else None
        self._file_series = peekable(self._stream_files(file_series)) if file_series else None

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
            and not self._files
            and not self._file_series
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
        size = self.populate_assign(
            update=update,
            assets=self._files,
            size=size,
        )
        size = self.populate_append(
            update=update,
            assets=self._file_series,
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

    def populate_append(
        self,
        update: UpdateRunSnapshot,
        assets: Optional[peekable[Value]],
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

            update.append[key].MergeFrom(value)
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

    def _stream_files(self, files: dict[str, FileRefData]) -> Iterator[tuple[str, Value]]:
        _is_instance = isinstance
        for attr_name, file in _validate_paths(files):
            if len(file.destination) > MAX_FILE_DESTINATION_LENGTH:
                _warn_or_raise_on_invalid_value(
                    f"File destination must be a string of at most {MAX_FILE_DESTINATION_LENGTH} characters"
                    f"(got `{file.destination}` for {attr_name}`)"
                )
                continue

            if len(file.mime_type) > MAX_FILE_MIME_TYPE_LENGTH:
                _warn_or_raise_on_invalid_value(
                    f"File mime type must be a string of at most {MAX_FILE_MIME_TYPE_LENGTH} characters"
                    f" (got `{file.mime_type}` for `{attr_name}`)"
                )
                continue

            yield (
                attr_name,
                Value(file_ref=FileRef(path=file.destination, mime_type=file.mime_type, size_bytes=file.size_bytes)),
            )


def sanitize_attribute_path(param: str) -> str:
    if len(param) > MAX_ATTRIBUTE_PATH_LENGTH:
        param = param[:MAX_ATTRIBUTE_PATH_LENGTH]
    return param


def _validate_paths(fields: dict[str, T]) -> Iterator[tuple[str, T]]:
    # local bindings, faster in tight loops
    _is_instance = isinstance
    __is_over_utf8_bytes_limit = _is_over_utf8_bytes_limit
    _max_length = MAX_ATTRIBUTE_PATH_LENGTH

    for key, value in fields.items():
        if not _is_instance(key, str):
            _warn_or_raise_on_invalid_value(f"Field paths must be strings (got `{key}`)")
            continue

        if __is_over_utf8_bytes_limit(key, _max_length):
            _warn_or_raise_on_invalid_value(
                f"Field paths must be less than {_max_length} bytes when UTF-8 encoded (got `{key}`)"
            )
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
    string_series: Optional[dict[str, str]],
    step: Optional[Union[float, int]],
    timestamp: datetime,
    max_size: int = MAX_SINGLE_OPERATION_SIZE_BYTES,
) -> Iterator[UpdateRunSnapshot]:
    if not string_series:
        return
    assert step is not None, "Step must be provided when string series are present"

    stream = peekable(_stream_string_series(string_series))
    step = make_step(step)
    timestamp = datetime_to_proto(timestamp)

    # Local bindings for faster name lookups
    _proto_string_size = proto_string_size
    _peek_stream = stream.peek
    while stream:
        update = UpdateRunSnapshot(step=step, timestamp=timestamp)

        size = 0
        while size < max_size:
            try:
                key, value = _peek_stream()
            except StopIteration:
                break

            new_size = size + _proto_string_size(key) + _proto_string_size(value) + 6
            if new_size > max_size:
                break

            update.append[key].string = value
            size, _ = new_size, next(stream)

        yield update


def _stream_string_series(string_series: dict[str, str]) -> Iterator[tuple[str, str]]:
    # local bindings, faster in tight loops
    _is_instance = isinstance
    __is_over_utf8_bytes_limit = _is_over_utf8_bytes_limit
    _max_length = MAX_STRING_SERIES_DATA_POINT_LENGTH

    for key, value in _validate_paths(string_series):
        if not _is_instance(value, str):
            _warn_or_raise_on_invalid_value(f"String series values must be strings (got `{key}`:`{value}`)")
            continue

        if __is_over_utf8_bytes_limit(value, _max_length):
            _warn_or_raise_on_invalid_value(
                f"String series values must be less than {_max_length} bytes when UTF-8 encoded"
            )
            continue

        yield key, value


def histograms_to_update_run_snapshots(
    histograms: Optional[dict[str, Histogram]],
    step: Optional[Union[float, int]],
    timestamp: datetime,
    max_size: int = MAX_SINGLE_OPERATION_SIZE_BYTES,
) -> Iterator[UpdateRunSnapshot]:
    if not histograms:
        return

    assert step is not None, "Step must be provided when histograms are present"

    stream = peekable(_stream_histograms(histograms))
    step = make_step(step)
    timestamp = datetime_to_proto(timestamp)

    # Local bindings for faster name lookups
    _proto_string_size = proto_string_size
    _peek_stream = stream.peek
    while stream:
        update = UpdateRunSnapshot(step=step, timestamp=timestamp)

        size = 0
        while size < max_size:
            try:
                key, histogram = _peek_stream()
            except StopIteration:
                break

            new_size = size + _proto_string_size(key) + histogram.ByteSize() + 6
            if new_size > max_size:
                break

            update.append[key].histogram.CopyFrom(histogram)
            size, _ = new_size, next(stream)

        yield update


def _stream_histograms(histograms: dict[str, Histogram]) -> Iterator[tuple[str, ProtobufHistogram]]:
    # local bindings, faster in tight loops
    _is_instance = isinstance
    _max_histogram_bin_edges = MAX_HISTOGRAM_BIN_EDGES

    for key, value in _validate_paths(histograms):
        if not _is_instance(value, Histogram):
            _warn_or_raise_on_invalid_value(f"Histogram values must be of type Histogram (got `{key}`:`{value}`)")
            continue

        has_counts, has_densities = value.counts is not None, value.densities is not None
        values_field = value.counts if has_counts else value.densities

        try:
            bin_edges = value.bin_edges_as_list()
            counts = value.counts_as_list() if value.counts is not None else None
            densities = value.densities_as_list() if value.densities is not None else None
        except TypeError as e:
            _warn_or_raise_on_invalid_value(f"{e} (at `{key}`)")
            continue

        # Merge the check for 'either one but not both is set' into a single condition
        if has_counts == has_densities:
            _warn_or_raise_on_invalid_value(
                f"One of Histogram counts and densities must be set, and they cannot be set together (at `{key}`)"
            )
            continue

        if not bin_edges or len(bin_edges) > _max_histogram_bin_edges:
            _warn_or_raise_on_invalid_value(
                f"Histogram bin edges must not be empty and must have at most {_max_histogram_bin_edges} elements "
                f"(got {len(value.bin_edges)} bin edges at `{key}`)"
            )
            continue

        if len(values_field) != len(value.bin_edges) - 1:  # type: ignore
            field_name = "counts" if has_counts else "densities"
            _warn_or_raise_on_invalid_value(
                f"Histogram {field_name} must be of length equal to bin_edges - 1 "
                f"(got {len(values_field)} {field_name} and {len(value.bin_edges)} bin edges at `{key}`)"  # type: ignore
            )
            continue

        try:
            histogram = ProtobufHistogram(
                bin_edges=bin_edges,
                counts=ProtobufHistogram.Counts(values=counts) if has_counts else None,
                densities=ProtobufHistogram.Densities(values=densities) if has_densities else None,
            )
        # TypeError is raised by protobuf when the values are not numeric, also if counts are not int
        except TypeError:
            _warn_or_raise_on_invalid_value(
                f"Histogram bin_edges and densities must be numeric, counts must be int, if provided (at `{key}`)"
            )
            continue

        yield key, histogram


def _is_over_utf8_bytes_limit(string: str, max_bytes: int) -> bool:
    """Return True if a given string can NOT fit into `max_bytes` when encoded as UTF-8"""

    # Don't encode the string if it is shorter than this value.
    # A UTF8 encoded character can take up to 4 bytes, so if we know the string will fit,
    # there's no need to encode to check length.
    encode_length_threshold = max_bytes // 4

    return len(string) > encode_length_threshold and len(string.encode("utf-8")) > max_bytes


def datetime_to_proto(dt: datetime) -> Timestamp:
    dt_ts = dt.timestamp()
    return Timestamp(seconds=int(dt_ts), nanos=int((dt_ts % 1) * 1e9))


def decompose_step(step: Union[float, int]) -> tuple[int, int]:
    """Decompose a number representing a step into whole and micro parts."""

    m = 1e6
    micro_total = int(round(step * m, 6))
    whole = int(micro_total // m)
    micro = int(micro_total % m)

    return whole, micro


def make_step(number: Union[float, int]) -> Step:
    """
    Converts a number to protobuf Step value. Example:
    >>> assert make_step(7.654321) == Step(whole=7, micro=654321)

    Args:
        number: step expressed as number
        raise_on_step_precision_loss: inform converter whether it should silently drop precision and
            round down to 6 decimal places or raise an error.

    Returns: Step protobuf used in Neptune API.
    """
    whole, micro = decompose_step(number)
    return Step(whole=whole, micro=micro)


def _warn_or_raise_on_invalid_value(message: str) -> None:
    if INVALID_VALUE_ACTION == "drop":
        logger.warning(f"Dropping value. {message}.")
    else:
        raise NeptuneUnableToLogData(message)
