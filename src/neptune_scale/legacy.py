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

import neptune_scale.api.run

__all__ = ("Run",)

from neptune_scale.sync.metadata_splitter import Metrics


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
                "These arguments are not supported by Neptune and will be ignored."
            )

        return fn(*args, **kwargs)

    return wrapper


class Attribute:
    """Objects of this class are returned on dict-like access to Run. Attributes have a path and
    allow logging values under it. Example:

        run = Run(...)
        run['foo'] = 1
        run['nested'] = {'foo': 1, {'bar': {'baz': 2}}}
        run['bar'].append(1, step=10)
    """

    def __init__(
        self,
        run: "Run",
        path: str,
    ) -> None:
        self._log = run._log
        self._path = path

    @warn_unsupported_params
    def assign(self, value: Any, *, wait: bool = False) -> None:
        self._log(configs=accumulate_dict_values(value, self._path))

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
        if isinstance(timestamp, float):
            timestamp = datetime.fromtimestamp(timestamp)

        self._log(
            timestamp=timestamp,
            step=step,
            metrics=Metrics(
                data=accumulate_dict_values(value, self._path),
                preview=preview,
                preview_completion=preview_completion,
            ),
        )

    @warn_unsupported_params
    def add(self, values: Union[str, Union[list[str], set[str], tuple[str]]], *, wait: bool = False) -> None:
        if isinstance(values, str):
            values = (values,)
        self._log(tags_add={self._path: values})

    @warn_unsupported_params
    def remove(self, values: Union[str, Union[list[str], set[str], tuple[str]]], *, wait: bool = False) -> None:
        if isinstance(values, str):
            values = (values,)
        self._log(tags_remove={self._path: values})

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

    @staticmethod
    def _validate_lists_length_equal(values: Collection[Any], other: Optional[Collection[Any]], name: str) -> None:
        if other is not None:
            if len(other) != len(values):
                raise ValueError(f"All lists provided to extend must have the same length; len(value) != len({name})")


class Run(neptune_scale.api.run.Run):
    """This class extends the main Run class with a dict-like API compatible (on a basic level)
    with the legacy neptune-client package.

    Example:

        from neptune_scale.legacy import Run

        run = Run(...)
        run['foo'] = 1
        run['metrics/loss'].append(0.5, step=10)

        run.close()
    """

    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)
        self._attributes: dict[str, Attribute] = {}

    def __getitem__(self, path: str) -> "Attribute":
        path = cleanup_path(path)
        attr = self._attributes.get(path)
        if attr is None:
            attr = Attribute(self, path)
            self._attributes[path] = attr

        return attr

    def __setitem__(self, key: str, value: Any) -> None:
        # TODO: validate type if attr is already known?
        attr = self[key]
        attr.assign(value)


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


def accumulate_dict_values(value: Union[Any, dict[str, Any]], path_or_base: str) -> dict[str, Any]:
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


def iter_nested(dict_: dict[str, Any], path: str) -> Iterator[tuple[tuple[str, ...], Any]]:
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


def _iter_nested(dict_: dict[str, Any], path_acc: tuple[str, ...]) -> Iterator[tuple[tuple[str, ...], Any]]:
    if not dict_:
        raise ValueError("The dictionary cannot be empty or contain empty nested dictionaries.")

    for key, value in dict_.items():
        current_path = path_acc + (key,)
        if isinstance(value, dict):
            yield from _iter_nested(value, current_path)
        else:
            yield current_path, value
