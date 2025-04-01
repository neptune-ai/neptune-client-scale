from __future__ import annotations

import os

from neptune_scale.sync.parameters import MAX_SINGLE_OPERATION_SIZE_BYTES

__all__ = ("MetadataSplitter", "datetime_to_proto", "make_step")

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

from neptune_scale.api.metrics import Metrics
from neptune_scale.exceptions import (
    NeptuneFloatValueNanInfUnsupported,
    NeptuneScaleWarning,
)
from neptune_scale.util import (
    envs,
    get_logger,
)

logger = get_logger()

T = TypeVar("T")

SINGLE_FLOAT_VALUE_SIZE = Value(float64=1.0).ByteSize()


def _invalid_value_action_from_env() -> str:
    action = os.getenv(envs.INVALID_VALUE_ACTION, "raise")
    if action not in ("drop", "raise"):
        raise ValueError(f"Invalid value '{action}' for {envs.INVALID_VALUE_ACTION}. Must be 'drop' or 'raise'.")

    return action


INVALID_VALUE_ACTION = _invalid_value_action_from_env()
SHOULD_SKIP_NON_FINITE_METRICS = envs.get_bool(envs.SKIP_NON_FINITE_METRICS, True)


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
        self._metrics_preview = metrics.preview if metrics is not None else False
        self._metrics_preview_completion = metrics.preview_completion if metrics is not None else 0.0

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
        size = self.populate_append(
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

    def populate_append(
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

    def _stream_configs(
        self, configs: dict[str, Union[float, bool, int, str, datetime, list, set, tuple]]
    ) -> Iterator[tuple[str, Value]]:
        _is_instance = isinstance  # local binding, faster in tight loops
        for key, value in self._validate_paths(configs):
            if _is_instance(value, float):
                yield key, Value(float64=value)  # type: ignore
            elif _is_instance(value, bool):
                yield key, Value(bool=value)  # type: ignore
            elif _is_instance(value, int):
                yield key, Value(int64=value)  # type: ignore
            elif _is_instance(value, str):
                yield key, Value(string=value)  # type: ignore
            elif _is_instance(value, datetime):
                yield key, Value(timestamp=datetime_to_proto(value))  # type: ignore
            elif _is_instance(value, (list, set, tuple)):
                yield key, Value(string_set=StringSet(values=value))  # type: ignore
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


def proto_string_size(key: str) -> int:
    """
    Calculates the size of the string in the protobuf message including an overhead of the length prefix (varint)
        with an assumption of maximal string length.
    """
    key_bin = bytes(key, "utf-8")
    return len(key_bin) + 2 + (1 if len(key_bin) > 127 else 0)


def _warn_or_raise_on_invalid_value(message: str) -> None:
    if INVALID_VALUE_ACTION == "raise":
        raise TypeError(message)
    else:
        logger.warning(f"Dropping value. {message}.")
