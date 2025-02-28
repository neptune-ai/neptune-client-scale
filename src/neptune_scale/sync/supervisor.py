from __future__ import annotations

__all__ = ("ProcessSupervisor",)

from multiprocessing import Process
from collections.abc import Callable

from neptune_scale.util import (
    Daemon,
    get_logger,
)

logger = get_logger()


class ProcessSupervisor(Daemon):
    def __init__(
        self,
        process: Process,
        callback: Callable[[], None]
    ) -> None:
        super().__init__(name="ProcessSupervisor", sleep_time=0.5)
        self._process: Process = sync_process
        self._callback: Callable[[], None] = callback

    def work(self) -> None:
        if not self._process.is_alive():
            logger.error(f"{self._process.name} is not alive.")
            self._callback()
