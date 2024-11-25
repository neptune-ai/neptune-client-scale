import functools
import itertools
import warnings
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Collection,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)

if TYPE_CHECKING:
    from neptune_scale.api.run import Run


def warn_unsupported_params(fn: Callable) -> Callable:
    # Perform some simple heuristics to detect if a method is called with parameters
    # that are not supported by Scale
    warn = functools.partial(warnings.warn, stacklevel=3)

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
ValueType = Any  # Union[float, int, str, bool, datetime, Tuple, List, Dict, Set]


class AttributeStore:
    def __init__(self, run: "Run") -> None:
        self._run = run
        self._attributes: Dict[str, Attribute] = {}

    def __getitem__(self, key: str) -> "Attribute":
        path = cleanup_path(key)
        attr = self._attributes.get(key)
        if attr is None:
            attr = Attribute(self, path)
            self._attributes[path] = attr

        return attr

    def __setitem__(self, key: str, value: ValueType) -> None:
        # TODO: validate type if attr is already known
        attr = self[key]
        attr.assign(value)

    def log(
        self,
        step: Optional[Union[float, int]] = None,
        timestamp: Optional[Union[datetime, float]] = None,
        configs: Optional[Dict[str, Union[float, bool, int, str, datetime, list, set, tuple]]] = None,
        metrics: Optional[Dict[str, Union[float, int]]] = None,
        tags_add: Optional[Dict[str, Union[List[str], Set[str], Tuple[str]]]] = None,
        tags_remove: Optional[Dict[str, Union[List[str], Set[str], Tuple[str]]]] = None,
    ) -> None:
        # TODO: This should not call Run.log, but do the actual work. Reverse the current dependency so that this
        #       class handles all the logging
        timestamp = datetime.now() if timestamp is None else timestamp

        # TODO: Remove this and teach MetadataSplitter to handle Nones
        configs = {} if configs is None else configs
        metrics = {} if metrics is None else metrics
        tags_add = {} if tags_add is None else tags_add
        tags_remove = {} if tags_remove is None else tags_remove

        print(f"log({step}, {configs}, {metrics}, {tags_add}, {tags_remove}")

        # TODO: remove once Run.log accepts Union[datetime, float]
        timestamp = cast(datetime, timestamp)
        self._run.log(
            step=step, timestamp=timestamp, configs=configs, metrics=metrics, tags_add=tags_add, tags_remove=tags_remove
        )


class Attribute:
    """Objects of this class are returned on dict-like access to Run. Attributes have a path and
    allow logging values under it.

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
        value: Union[Dict[str, Any], float],
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
    def add(self, values: Union[str, Union[List[str], Set[str], Tuple[str]]], *, wait: bool = False) -> None:
        if isinstance(values, str):
            values = (values,)
        self._store.log(tags_add={self._path: values})

    @warn_unsupported_params
    # TODO: this should be Iterable in Run as well
    # def remove(self, values: Union[str, Iterable[str]], *, wait: bool = False) -> None:
    def remove(self, values: Union[str, Union[List[str], Set[str], Tuple[str]]], *, wait: bool = False) -> None:
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


def iter_nested(dict_: Dict[str, ValueType], path: str) -> Iterator[Tuple[Tuple[str, ...], ValueType]]:
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


def _iter_nested(dict_: Dict[str, ValueType], path_acc: Tuple[str, ...]) -> Iterator[Tuple[Tuple[str, ...], ValueType]]:
    if not dict_:
        raise ValueError("The dictionary cannot be empty or contain empty nested dictionaries.")

    for key, value in dict_.items():
        current_path = path_acc + (key,)
        if isinstance(value, dict):
            yield from _iter_nested(value, current_path)
        else:
            yield current_path, value


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
    """

    path = path.strip()
    if path in ("", "/"):
        raise ValueError(f"Invalid path: `{path}`.")

    if path.startswith("/"):
        path = path[1:]

    if path.endswith("/"):
        raise ValueError(f"Invalid path: `{path}`. Path must not end with a slash.")
    if not all(path.split("/")):
        raise ValueError(f"Invalid path: `{path}`. Path components must not be empty.")

    return path


def accumulate_dict_values(value: Union[ValueType, Dict[str, ValueType]], path_or_base: str) -> Dict:
    """
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
