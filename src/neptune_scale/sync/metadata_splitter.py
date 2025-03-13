from __future__ import annotations

from neptune_scale.sync.parameters import MAX_SINGLE_OPERATION_SIZE_BYTES

__all__ = ("MetadataSplitter",)

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

from more_itertools import peekable
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
    SET_OPERATION,
    Preview,
    UpdateRunSnapshot,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale.api.metrics import Metrics
from neptune_scale.exceptions import (
    NeptuneFloatValueNanInfUnsupported,
    NeptuneScaleWarning,
)
from neptune_scale.net.serialization import (
    datetime_to_proto,
    make_step,
    make_value,
    pb_key_size,
)
from neptune_scale.util import (
    envs,
    get_logger,
)

logger = get_logger()

T = TypeVar("T", bound=Any)


SINGLE_FLOAT_VALUE_SIZE = make_value(1.0).ByteSize()


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
        self._should_skip_non_finite_metrics = envs.get_bool(envs.SKIP_NON_FINITE_METRICS, True)
        self._step = make_step(number=metrics.step) if (metrics is not None and metrics.step is not None) else None
        self._timestamp = datetime_to_proto(timestamp)
        self._project = project
        self._run_id = run_id
        self._configs = peekable(configs.items()) if configs else None
        self._metrics_data = (
            peekable(self._skip_non_finite(metrics.step, metrics.data)) if metrics is not None else None
        )
        self._metrics_preview = metrics.preview if metrics is not None else False
        self._metrics_preview_completion = metrics.preview_completion if metrics is not None else 0.0
        self._add_tags = peekable(add_tags.items()) if add_tags else None
        self._remove_tags = peekable(remove_tags.items()) if remove_tags else None

        self._max_update_bytes_size = (
            max_message_bytes_size
            - RunOperation(
                project=self._project,
                run_id=self._run_id,
                update=self._make_empty_update_snapshot(),
            ).ByteSize()
        )
        self._has_returned = False

    def __iter__(self) -> MetadataSplitter:
        self._has_returned = False
        return self

    def __next__(self) -> UpdateRunSnapshot:
        update = self._make_empty_update_snapshot()
        size = update.ByteSize()

        size = self.populate_assign(
            update=update,
            assets=self._configs,
            size=size,
        )
        size = self.populate_append(
            update=update,
            assets=self._metrics_data,
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

        if not self._has_returned or update.assign or update.append or update.modify_sets:
            self._has_returned = True
            return update
        else:
            raise StopIteration

    def populate_assign(
        self,
        update: UpdateRunSnapshot,
        assets: Optional[peekable[tuple[str, Any]]],
        size: int,
    ) -> int:
        if not assets:
            return size

        while size < self._max_update_bytes_size:
            try:
                key, value = assets.peek()
            except StopIteration:
                break

            proto_value = make_value(value)
            new_size = size + pb_key_size(key) + proto_value.ByteSize() + 6

            if new_size > self._max_update_bytes_size:
                break

            update.assign[key].MergeFrom(proto_value)
            size, _ = new_size, next(assets)

        return size

    def populate_append(
        self,
        update: UpdateRunSnapshot,
        assets: Optional[peekable[tuple[str, float]]],
        size: int,
    ) -> int:
        if not assets:
            return size

        while size < self._max_update_bytes_size:
            try:
                key, value = assets.peek()
            except StopIteration:
                break

            new_size = size + pb_key_size(key) + SINGLE_FLOAT_VALUE_SIZE + 6
            if new_size > self._max_update_bytes_size:
                break

            update.append[key].float64 = value
            size, _ = new_size, next(assets)

        return size

    def populate_tags(
        self, update: UpdateRunSnapshot, assets: Optional[peekable[Any]], operation: SET_OPERATION.ValueType, size: int
    ) -> int:
        if not assets:
            return size

        while size < self._max_update_bytes_size:
            try:
                key, values = assets.peek()
            except StopIteration:
                break

            if not isinstance(values, peekable):
                values = peekable(values)

            is_full = False
            new_size = size + pb_key_size(key) + 6
            for value in values:
                tag_size = pb_key_size(value) + 6
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

    def _skip_non_finite(self, step: Optional[float | int], metrics: dict[str, float]) -> Iterator[tuple[str, float]]:
        """Yields (metric, value) pairs, skipping non-finite values depending on the env setting."""

        for k, v in metrics.items():
            v = float(v)

            if not math.isfinite(v):
                if self._should_skip_non_finite_metrics:
                    warnings.warn(
                        f"Neptune is skipping non-finite metric values. You can turn this warning into an error by "
                        f"setting the `{envs.SKIP_NON_FINITE_METRICS}` environment variable to `False`.",
                        category=NeptuneScaleWarning,
                        stacklevel=7,
                    )

                    logger.warning(f"Skipping a non-finite value `{v}` of metric `{k}` at step `{step}`. ")
                    continue
                else:
                    raise NeptuneFloatValueNanInfUnsupported(metric=k, step=step, value=v)

            yield k, v

    def _make_empty_update_snapshot(self) -> UpdateRunSnapshot:
        include_preview = self._metrics_data and self._metrics_preview
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
