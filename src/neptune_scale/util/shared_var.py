import multiprocessing
from collections.abc import Callable
from types import TracebackType
from typing import (
    Generic,
    Optional,
    TypeVar,
    cast,
)

__all__ = ("SharedInt", "SharedFloat", "SharedVar")

T = TypeVar("T", int, float)


class SharedVar(Generic[T]):
    """A convenience wrapper around multiprocessing.Value and multiprocessing.Condition.

    Access the Value using the `value` property. Other methods, as well as context management
    delegate to the Condition object.

    Typical usage:
        var = SharedInt(0)

        # In one process
        with var:
            var.value += 1
            var.notify() # Notify the waiting process

        # In another process
        with var:
            var.wait() # Wait until the value is changed
            print(var.value)
    """

    def __init__(self, typecode_or_type: str, initial_value: T):
        self._value = multiprocessing.Value(typecode_or_type, initial_value)
        self._condition = multiprocessing.Condition(self._value.get_lock())

    @property
    def value(self) -> T:
        with self._condition:
            return cast(T, self._value.value)

    @value.setter
    def value(self, new_value: T) -> None:
        with self._condition:
            self._value.value = new_value

    def wait(self, timeout: Optional[float] = None) -> None:
        with self._condition:
            self._condition.wait(timeout)

    def wait_for(self, predicate: Callable[[], bool], timeout: Optional[float] = None) -> None:
        with self._condition:
            self._condition.wait_for(predicate, timeout)

    def notify_all(self) -> None:
        with self._condition:
            self._condition.notify_all()

    def __enter__(self) -> "SharedVar[T]":
        self._condition.__enter__()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self._condition.__exit__(exc_type, exc_value, traceback)


class SharedInt(SharedVar[int]):
    def __init__(self, initial_value: int) -> None:
        super().__init__("i", initial_value)


class SharedFloat(SharedVar[float]):
    def __init__(self, initial_value: float) -> None:
        super().__init__("d", initial_value)
