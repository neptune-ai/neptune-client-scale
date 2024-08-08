from __future__ import annotations

__all__ = (
    "MetadataSplitter",
    "NoSplitting",
)

from datetime import datetime
from typing import (
    Iterator,
    Protocol,
)

from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import UpdateRunSnapshot
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale.core.serialization import (
    datetime_to_proto,
    make_step,
    make_value,
    mod_tags,
)


class MetadataSplitter(Iterator[RunOperation], Protocol):
    """
    The Protocol Buffer messages are limited in size, so we need to split the metadata into smaller parts.
    """

    ...


class NoSplitting(MetadataSplitter):
    """
    Dummy splitter that does not split the metadata.
    """

    def __init__(
        self,
        *,
        project: str,
        run_id: str,
        step: int | float | None,
        timestamp: datetime,
        fields: dict[str, float | bool | int | str | datetime | list | set],
        metrics: dict[str, float],
        add_tags: dict[str, list[str] | set[str]],
        remove_tags: dict[str, list[str] | set[str]],
    ):
        self._step = None if step is None else make_step(number=step)
        self._timestamp = datetime_to_proto(timestamp)
        self._fields = fields
        self._metrics = metrics
        self._add_tags = add_tags
        self._remove_tags = remove_tags
        self._project = project
        self._run_id = run_id

        self._messages = [self._build_message()]

    def _build_message(self) -> RunOperation:
        modify_sets = {key: mod_tags(add=add) for key, add in self._add_tags.items()}
        modify_sets.update({key: mod_tags(remove=remove) for key, remove in self._remove_tags.items()})
        update = UpdateRunSnapshot(
            step=self._step,
            timestamp=self._timestamp,
            assign={key: make_value(value) for key, value in self._fields.items()},
            append={key: make_value(value) for key, value in self._metrics.items()},
            modify_sets=modify_sets,
        )

        return RunOperation(project=self._project, run_id=self._run_id, update=update)

    def __iter__(self) -> MetadataSplitter:
        return self

    def __next__(self) -> RunOperation:
        try:
            return self._messages.pop()
        except IndexError:
            raise StopIteration
