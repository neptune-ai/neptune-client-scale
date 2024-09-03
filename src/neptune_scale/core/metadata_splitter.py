from __future__ import annotations

__all__ = ("MetadataSplitter",)

from datetime import datetime
from itertools import starmap
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
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

from neptune_scale.core.serialization import (
    datetime_to_proto,
    make_step,
    make_value,
    pb_key_size,
)

T = TypeVar("T", bound=Any)


class MetadataSplitter(Iterator[RunOperation]):
    def __init__(
        self,
        *,
        project: str,
        run_id: str,
        step: Optional[Union[int, float]],
        timestamp: datetime,
        fields: Dict[str, Union[float, bool, int, str, datetime, list, set]],
        metrics: Dict[str, float],
        add_tags: Dict[str, Union[List[str], Set[str]]],
        remove_tags: Dict[str, Union[List[str], Set[str]]],
        max_message_bytes_size: int = 1024 * 1024,
    ):
        self._step = None if step is None else make_step(number=step)
        self._timestamp = datetime_to_proto(timestamp)
        self._project = project
        self._run_id = run_id
        self._fields = peekable(fields.items())
        self._metrics = peekable(starmap(lambda k, v: (k, float(v)), metrics.items()))
        self._add_tags = peekable(add_tags.items())
        self._remove_tags = peekable(remove_tags.items())

        self._max_update_bytes_size = (
            max_message_bytes_size
            - RunOperation(
                project=self._project,
                run_id=self._run_id,
                update=UpdateRunSnapshot(step=self._step, timestamp=self._timestamp),
            ).ByteSize()
        )

        self._has_returned = False

    def __iter__(self) -> MetadataSplitter:
        self._has_returned = False
        return self

    def __next__(self) -> RunOperation:
        size = 0
        update = UpdateRunSnapshot(
            step=self._step,
            timestamp=self._timestamp,
            assign={},
            append={},
            modify_sets={},
        )

        size = self.populate(
            assets=self._fields,
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
        _ = self.populate_tags(
            update=update,
            assets=self._remove_tags,
            operation=SET_OPERATION.REMOVE,
            size=size,
        )

        if not self._has_returned or update.assign or update.append or update.modify_sets:
            self._has_returned = True
            return RunOperation(project=self._project, run_id=self._run_id, update=update)
        else:
            raise StopIteration

    def populate(
        self,
        assets: peekable[Any],
        update_producer: Callable[[str, Value], None],
        size: int,
    ) -> int:
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
        self, update: UpdateRunSnapshot, assets: peekable[Any], operation: SET_OPERATION.ValueType, size: int
    ) -> int:
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
