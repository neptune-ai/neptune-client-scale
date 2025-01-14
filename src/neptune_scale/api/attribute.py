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

import functools
import itertools
import threading
import warnings
from collections.abc import (
    Collection,
    Iterator,
)
from datetime import datetime
from typing import (
    Any,
    Callable,
    Optional,
    Union,
    cast,
)

from neptune_scale.exceptions import NeptuneSeriesStepNonIncreasing
from neptune_scale.sync.metadata_splitter import MetadataSplitter
from neptune_scale.sync.operations_queue import OperationsQueue

__all__ = ("Attribute", "AttributeStore")


def warn_unsupported_params(fn: Callable) -> Callable:
    # Perform some simple heuristics to detect if a method is called with parameters
    # that are not supported by Scale
    warn = functools.partial(warnings.warn, stacklevel=3)

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):  # type: ignore
        if kwargs.get("wait") is not None:
            warn("The `wait` parameter is not yet implemented and will be ignored.")

        extra_kwargs = set(kwargs.keys()) - {"wait", "step", "timestamp", "steps", "timestamps"}
        if extra_kwargs:
            warn(
                f"`{fn.__name__}()` was called with additional keyword argument(s): `{', '.join(extra_kwargs)}`. "
                "These arguments are not supported by Neptune Scale and will be ignored."
            )

        return fn(*args, **kwargs)

    return wrapper


# TODO: proper typehinting
# AtomType = Union[float, bool, int, str, datetime, list, set, tuple]
ValueType = Any  # Union[float, int, str, bool, datetime, Tuple, List, Dict, Set]


class AttributeStore:
    """
    Responsible for managing local attribute store, and pushing log() operations
    to the provided OperationsQueue -- assuming that there is something on the other
    end consuming the queue (which would be SyncProcess).
    """

    def __init__(self, project: str, run_id: str, operations_queue: OperationsQueue) -> None:
        self._project = project
        self._run_id = run_id
        self._operations_queue = operations_queue
        self._attributes: dict[str, Attribute] = {}
        # Keep a list of path -> (last step, last value) mappings to detect non-increasing steps
        # at call site. The backend will detect this error as well, but it's more convenient for the user
        # to get the error as soon as possible.
        self._metric_state: dict[str, tuple[float, float]] = {}
        self._lock = threading.RLock()

    def __getitem__(self, path: str) -> "Attribute":
        path = cleanup_path(path)
        attr = self._attributes.get(path)
        if attr is None:
            attr = Attribute(self, path)
            self._attributes[path] = attr

        return attr

    def __setitem__(self, key: str, value: ValueType) -> None:
        # TODO: validate type if attr is already known?
        attr = self[key]
        attr.assign(value)

    def log(
        self,
        step: Optional[Union[float, int]] = None,
        timestamp: Optional[Union[datetime, float]] = None,
        configs: Optional[dict[str, Union[float, bool, int, str, datetime, list, set, tuple]]] = None,
        metrics: Optional[dict[str, Union[float, int]]] = None,
        tags_add: Optional[dict[str, Union[list[str], set[str], tuple[str]]]] = None,
        tags_remove: Optional[dict[str, Union[list[str], set[str], tuple[str]]]] = None,
    ) -> None:
        if timestamp is None:
            timestamp = datetime.now()
        elif isinstance(timestamp, (float, int)):
            timestamp = datetime.fromtimestamp(timestamp)

        # MetadataSplitter is an iterator, so gather everything into a list instead of iterating over
        # it in the critical section, to avoid holding the lock for too long.
        # TODO: Move splitting into the worker process. Here we should just send messages as they are.
        chunks = list(
            MetadataSplitter(
                project=self._project,
                run_id=self._run_id,
                step=step,
                timestamp=timestamp,
                configs=configs,
                metrics=metrics,
                add_tags=tags_add,
                remove_tags=tags_remove,
            )
        )

        with self._lock:
            self._verify_and_update_metrics_state(step, metrics)

            for operation, metadata_size in chunks:
                self._operations_queue.enqueue(operation=operation, size=metadata_size)

    def _verify_and_update_metrics_state(self, step: Optional[float], metrics: Optional[dict[str, float]]) -> None:
        """Check if step in provided metrics is increasing, raise `NeptuneSeriesStepNonIncreasing` if not."""

        if step is None or metrics is None:
            return

        for metric, value in metrics.items():
            if (state := self._metric_state.get(metric)) is not None:
                last_step, last_value = state
                # Repeating a step is fine as long as the value does not change
                if step < last_step or (step == last_step and value != last_value):
                    raise NeptuneSeriesStepNonIncreasing()

            self._metric_state[metric] = (step, value)


class Attribute:
    """Objects of this class are returned on dict-like access to Run. Attributes have a path and
    allow logging values under it. Example:

        run = Run(...)
        run['foo'] = 1
        run['nested'] = {'foo': 1, {'bar': {'baz': 2}}}
        run['bar'].append(1, step=10)
    """

    def __init__(self, store: AttributeStore, path: str) -> None:
        self._store = store
        self._path = path

    # TODO: typehint value properly
    @warn_unsupported_params
    def assign(self, value: Any, *, wait: bool = False) -> None:
        data = accumulate_dict_values(value, self._path)
        self._store.log(configs=data)

    @warn_unsupported_params
    def append(
        self,
        value: Union[dict[str, Any], float],
        *,
        step: Union[float, int],
        timestamp: Optional[Union[float, datetime]] = None,
        wait: bool = False,
        **kwargs: Any,
    ) -> None:
        data = accumulate_dict_values(value, self._path)
        self._store.log(metrics=data, step=step, timestamp=timestamp)

    @warn_unsupported_params
    # TODO: this should be Iterable in Run as well
    # def add(self, values: Union[str, Iterable[str]], *, wait: bool = False) -> None:
    def add(self, values: Union[str, Union[list[str], set[str], tuple[str]]], *, wait: bool = False) -> None:
        if isinstance(values, str):
            values = (values,)
        self._store.log(tags_add={self._path: values})

    @warn_unsupported_params
    # TODO: this should be Iterable in Run as well
    # def remove(self, values: Union[str, Iterable[str]], *, wait: bool = False) -> None:
    def remove(self, values: Union[str, Union[list[str], set[str], tuple[str]]], *, wait: bool = False) -> None:
        if isinstance(values, str):
            values = (values,)
        self._store.log(tags_remove={self._path: values})

    @warn_unsupported_params
    def extend(
        self,
        values: Collection[Union[float, int]],
        *,
        steps: Collection[Union[float, int]],
        timestamps: Optional[Collection[Union[float, datetime]]] = None,
        wait: bool = False,
        **kwargs: Any,
    ) -> None:
        # TODO: make this compatible with the old client
        assert len(values) == len(steps)

        if timestamps is not None:
            assert len(timestamps) == len(values)
        else:
            timestamps = cast(tuple, itertools.repeat(datetime.now()))

        for value, step, timestamp in zip(values, steps, timestamps):
            self.append(value, step=step, timestamp=timestamp, wait=wait)

    # TODO: add value type validation to all the methods
    # TODO: change Run API to typehint timestamp as Union[datetime, float]


def cleanup_path(path: str) -> str:
    """
    >>> cleanup_path('/a/b/c')
    'a/b/c'
    >>> cleanup_path('a/b/c/')
    Traceback (most recent call last):
    ...
    ValueError: Invalid path: `a/b/c/`. Path must not end with a slash.
    >>> cleanup_path('a//b/c')
    Traceback (most recent call last):
    ...
    ValueError: Invalid path: `a//b/c`. Path components must not be empty.
    >>> cleanup_path('a/ /b/c')
    Traceback (most recent call last):
    ...
    ValueError: Invalid path: `a/ /b/c`. Path components cannot contain leading or trailing whitespace.
    >>> cleanup_path('a/b/c ')
    Traceback (most recent call last):
    ...
    ValueError: Invalid path: `a/b/c `. Path components cannot contain leading or trailing whitespace.
    """

    if path.strip() in ("", "/"):
        raise ValueError(f"Invalid path: `{path}`.")

    orig_parts = path.split("/")
    parts = [x.strip() for x in orig_parts]

    for i, part in enumerate(parts):
        if part != orig_parts[i]:
            raise ValueError(f"Invalid path: `{path}`. Path components cannot contain leading or trailing whitespace.")

    # Skip the first slash, if present
    if parts[0] == "":
        parts = parts[1:]

    if parts[-1] == "":
        raise ValueError(f"Invalid path: `{path}`. Path must not end with a slash.")

    if not all(parts):
        raise ValueError(f"Invalid path: `{path}`. Path components must not be empty.")

    return "/".join(parts)


def accumulate_dict_values(value: Union[ValueType, dict[str, ValueType]], path_or_base: str) -> dict[str, Any]:
    """
    If value is a dict, flatten nested dictionaries into a single dict with unwrapped paths, each
    starting with `path_or_base`.

    If value is an atom, return a dict with a single entry `path_or_base` -> `value`.

    >>> accumulate_dict_values(1, "foo")
    {'foo': 1}
    >>> accumulate_dict_values({"bar": 1, 'l0/l1': 2, 'l3':{"l4": 3}}, "foo")
    {'foo/bar': 1, 'foo/l0/l1': 2, 'foo/l3/l4': 3}
    """

    if isinstance(value, dict):
        data = {"/".join(path): value for path, value in iter_nested(value, path_or_base)}
    else:
        data = {path_or_base: value}

    return data


def iter_nested(dict_: dict[str, ValueType], path: str) -> Iterator[tuple[tuple[str, ...], ValueType]]:
    """Iterate a nested dictionary, yielding a tuple of path components and value.

    >>> list(iter_nested({"foo": 1, "bar": {"baz": 2}}, "base"))
    [(('base', 'foo'), 1), (('base', 'bar', 'baz'), 2)]
    >>> list(iter_nested({"foo":{"bar": 1}, "bar":{"baz": 2}}, "base"))
    [(('base', 'foo', 'bar'), 1), (('base', 'bar', 'baz'), 2)]
    >>> list(iter_nested({"foo": 1, "bar": 2}, "base"))
    [(('base', 'foo'), 1), (('base', 'bar'), 2)]
    >>> list(iter_nested({"foo": {}}, ""))
    Traceback (most recent call last):
    ...
    ValueError: The dictionary cannot be empty or contain empty nested dictionaries.
    """

    parts = tuple(path.split("/"))
    yield from _iter_nested(dict_, parts)


def _iter_nested(dict_: dict[str, ValueType], path_acc: tuple[str, ...]) -> Iterator[tuple[tuple[str, ...], ValueType]]:
    if not dict_:
        raise ValueError("The dictionary cannot be empty or contain empty nested dictionaries.")

    for key, value in dict_.items():
        current_path = path_acc + (key,)
        if isinstance(value, dict):
            yield from _iter_nested(value, current_path)
        else:
            yield current_path, value
