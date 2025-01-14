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

from typing import Any

import neptune_scale.api.run
from neptune_scale.api.attribute import Attribute

__all__ = ("Run",)


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

    def __getitem__(self, key: str) -> Attribute:
        return self._attr_store[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self._attr_store[key] = value
