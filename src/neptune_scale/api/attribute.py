import functools
import itertools
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
)

from neptune_scale.api.metrics import Metrics
from neptune_scale.sync.metadata_splitter import MetadataSplitter
from neptune_scale.sync.operations_repository import OperationsRepository
from neptune_scale.sync.sequence_tracker import SequenceTracker

__all__ = ("Attribute", "AttributeStore")


def warn_unsupported_params(fn: Callable) -> Callable:
    # Perform some simple heuristics to detect if a method is called with parameters
    # that are not supported by Scale
    warn = functools.partial(warnings.warn, stacklevel=3)

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):  # type: ignore
        if kwargs.get("wait") is not None:
            warn("The `wait` parameter is not yet implemented and will be ignored.")

        expected = {
            "wait",
            "step",
            "timestamp",
            "steps",
            "timestamps",
            "preview",
            "previews",
            "preview_completion",
            "preview_completions",
        }
        extra_kwargs = set(kwargs.keys()) - expected
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

    def __init__(
        self, project: str, run_id: str, operations_repo: OperationsRepository, sequence_tracker: SequenceTracker
    ) -> None:
        self._project = project
        self._run_id = run_id
        self._operations_repo = operations_repo
        self._attributes: dict[str, Attribute] = {}
        self._sequence_tracker = sequence_tracker

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
        timestamp: Optional[Union[datetime, float]] = None,
        configs: Optional[dict[str, Union[float, bool, int, str, datetime, list, set, tuple]]] = None,
        metrics: Optional[Metrics] = None,
        tags_add: Optional[dict[str, Union[list[str], set[str], tuple[str]]]] = None,
        tags_remove: Optional[dict[str, Union[list[str], set[str], tuple[str]]]] = None,
    ) -> None:
        if timestamp is None:
            timestamp = datetime.now()
        elif isinstance(timestamp, float):
            timestamp = datetime.fromtimestamp(timestamp)

        splitter: MetadataSplitter = MetadataSplitter(
            project=self._project,
            run_id=self._run_id,
            timestamp=timestamp,
            configs=configs,
            metrics=metrics,
            add_tags=tags_add,
            remove_tags=tags_remove,
        )

        operations = list(splitter)
        sequence_id = self._operations_repo.save_update_run_snapshots(operations)

        self._sequence_tracker.update_sequence_id(sequence_id)


class Attribute:
    """Objects of this class are returned on dict-like access to Run. Attributes have a path and
    allow logging values under it. Example:

        run = Run(...)
        run['foo'] = 1
        run['nested'] = {'foo': 1, {'bar': {'baz': 2}}}
        run['bar'].append(1, step=10)
    """

    def __init__(self, store: Optional[AttributeStore], path: str) -> None:
        self._store = store
        self._path = path

    # TODO: typehint value properly
    @warn_unsupported_params
    def assign(self, value: Any, *, wait: bool = False) -> None:
        data = accumulate_dict_values(value, self._path)
        if self._store is not None:
            self._store.log(configs=data)

    @warn_unsupported_params
    def append(
        self,
        value: Union[dict[str, Any], float],
        *,
        step: Union[float, int],
        preview: bool = False,
        preview_completion: Optional[float] = None,
        timestamp: Optional[Union[float, datetime]] = None,
        wait: bool = False,
        **kwargs: Any,
    ) -> None:
        data = accumulate_dict_values(value, self._path)
        if self._store is not None:
            self._store.log(
                metrics=Metrics(data=data, step=step, preview=preview, preview_completion=preview_completion),
                timestamp=timestamp,
            )

    @warn_unsupported_params
    # TODO: this should be Iterable in Run as well
    # def add(self, values: Union[str, Iterable[str]], *, wait: bool = False) -> None:
    def add(self, values: Union[str, Union[list[str], set[str], tuple[str]]], *, wait: bool = False) -> None:
        if isinstance(values, str):
            values = (values,)
        if self._store is not None:
            self._store.log(tags_add={self._path: values})

    @warn_unsupported_params
    # TODO: this should be Iterable in Run as well
    # def remove(self, values: Union[str, Iterable[str]], *, wait: bool = False) -> None:
    def remove(self, values: Union[str, Union[list[str], set[str], tuple[str]]], *, wait: bool = False) -> None:
        if isinstance(values, str):
            values = (values,)
        if self._store is not None:
            self._store.log(tags_remove={self._path: values})

    @warn_unsupported_params
    def extend(
        self,
        values: Collection[Union[float, int]],
        *,
        steps: Collection[Union[float, int]],
        timestamps: Optional[Collection[Union[float, datetime]]] = None,
        previews: Optional[Collection[bool]] = None,
        preview_completions: Optional[Collection[float]] = None,
        wait: bool = False,
        **kwargs: Any,
    ) -> None:
        # TODO: make this compatible with the old client
        self._validate_lists_length_equal(values, steps, "steps")
        self._validate_lists_length_equal(values, timestamps, "timestamps")
        self._validate_lists_length_equal(values, previews, "preview")
        self._validate_lists_length_equal(values, preview_completions, "preview_completions")
        if timestamps is None:
            timestamps = (datetime.now(),) * len(values)
        it = itertools.zip_longest(values, steps, timestamps, previews or [], preview_completions or [])
        for value, step, ts, preview, completion in it:
            kwargs = {}
            if preview is not None:
                kwargs["preview"] = preview
            if completion is not None:
                kwargs["preview_completion"] = completion
            self.append(value, step=step, timestamp=ts, wait=wait, **kwargs)

    # TODO: add value type validation to all the methods
    # TODO: change Run API to typehint timestamp as Union[datetime, float]

    def _validate_lists_length_equal(
        self, values: Collection[Any], other: Optional[Collection[Any]], name: str
    ) -> None:
        if other is not None:
            if len(other) != len(values):
                raise ValueError(f"All lists provided to extend must have the same length; len(value) != len({name})")


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
