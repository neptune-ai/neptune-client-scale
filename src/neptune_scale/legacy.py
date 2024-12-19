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
