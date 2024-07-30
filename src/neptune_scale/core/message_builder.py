from __future__ import annotations

__all__ = ("MessageBuilder",)

from datetime import datetime

from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
    SET_OPERATION,
    ModifySet,
    Step,
    StringSet,
    UpdateRunSnapshot,
    Value,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale.core.proto_utils import datetime_to_proto


class MessageBuilder:
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

        self._was_produced: bool = False

    def __iter__(self) -> MessageBuilder:
        return self

    def __next__(self) -> RunOperation:
        if not self._was_produced:
            self._was_produced = True

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

        raise StopIteration


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
