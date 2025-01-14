#
# Copyright (c) 2025, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

__all__ = ("MetadataSplitter",)

import math
import warnings
from collections.abc import (
    Callable,
    Iterator,
)
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
    UpdateRunSnapshot,
    Value,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

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


class MetadataSplitter(Iterator[tuple[RunOperation, int]]):
    def __init__(
        self,
        *,
        project: str,
        run_id: str,
        step: Optional[Union[int, float]],
        timestamp: datetime,
        configs: Optional[dict[str, Union[float, bool, int, str, datetime, list, set, tuple]]],
        metrics: Optional[dict[str, float]],
        add_tags: Optional[dict[str, Union[list[str], set[str], tuple[str]]]],
        remove_tags: Optional[dict[str, Union[list[str], set[str], tuple[str]]]],
        max_message_bytes_size: int = 1024 * 1024,
    ):
        self._step = None if step is None else make_step(number=step)
        self._timestamp = datetime_to_proto(timestamp)
        self._project = project
        self._run_id = run_id
        self._configs = peekable(configs.items()) if configs else None
        self._metrics = peekable(self._skip_non_finite(step, metrics)) if metrics else None
        self._add_tags = peekable(add_tags.items()) if add_tags else None
        self._remove_tags = peekable(remove_tags.items()) if remove_tags else None

        self._max_update_bytes_size = (
            max_message_bytes_size
            - RunOperation(
                project=self._project,
                run_id=self._run_id,
                update=UpdateRunSnapshot(step=self._step, timestamp=self._timestamp),
            ).ByteSize()
        )

        self._has_returned = False
        self._should_skip_non_finite_metrics = envs.get_bool(envs.SKIP_NON_FINITE_METRICS, True)

    def __iter__(self) -> MetadataSplitter:
        self._has_returned = False
        return self

    def __next__(self) -> tuple[RunOperation, int]:
        update = UpdateRunSnapshot(
            step=self._step,
            timestamp=self._timestamp,
            assign={},
            append={},
            modify_sets={},
        )
        size = update.ByteSize()

        size = self.populate(
            assets=self._configs,
            update_producer=lambda key, value: update.assign[key].MergeFrom(value),
            size=size,
        )
        size = self.populate(
            assets=self._metrics,
            update_producer=lambda key, value: update.append[key].MergeFrom(value),
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
            return RunOperation(project=self._project, run_id=self._run_id, update=update), size
        else:
            raise StopIteration

    def populate(
        self,
        assets: Optional[peekable[Any]],
        update_producer: Callable[[str, Value], None],
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

            update_producer(key, proto_value)
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
