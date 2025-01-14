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

from __future__ import annotations

from abc import (
    ABC,
    abstractmethod,
)
from types import TracebackType
from typing import Optional


class AutoCloseable(ABC):
    def __enter__(self) -> AutoCloseable:
        return self

    @abstractmethod
    def close(self) -> None: ...

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
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
