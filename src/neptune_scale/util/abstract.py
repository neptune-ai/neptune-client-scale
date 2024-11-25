from __future__ import annotations

from abc import (
    ABC,
    abstractmethod,
)
from types import TracebackType
from typing import (
    Optional,
    Type,
)


class AutoCloseable(ABC):
    def __enter__(self) -> AutoCloseable:
        return self

    @abstractmethod
    def close(self) -> None: ...

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.close()


class Resource(AutoCloseable):
    def cleanup(self) -> None:
        pass

    def flush(self) -> None:
        pass

    def close(self) -> None:
        self.flush()


class WithResources(Resource):
    @property
    @abstractmethod
    def resources(self) -> tuple[Resource, ...]: ...

    def flush(self) -> None:
        for resource in self.resources:
            resource.flush()

    def close(self) -> None:
        for resource in self.resources:
            resource.close()

    def cleanup(self) -> None:
        for resource in self.resources:
            resource.cleanup()
