from __future__ import annotations

from abc import (
    ABC,
    abstractmethod,
)
from types import TracebackType


class AutoCloseable(ABC):
    def __enter__(self) -> AutoCloseable:
        return self

    @abstractmethod
    def close(self) -> None: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


class Resource(AutoCloseable):
    @abstractmethod
    def cleanup(self) -> None: ...

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
