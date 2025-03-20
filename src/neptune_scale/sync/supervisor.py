from __future__ import annotations

__all__ = ("ProcessSupervisor",)

from collections.abc import Callable
from multiprocessing import Process

from neptune_scale.sync.parameters import PROCESS_SUPERVISOR_THREAD_SLEEP_TIME
from neptune_scale.util import (
    Daemon,
    get_logger,
)

logger = get_logger()


class ProcessSupervisor(Daemon):
    def __init__(self, process: Process, callback: Callable[[], None]) -> None:
        super().__init__(name="ProcessSupervisor", sleep_time=PROCESS_SUPERVISOR_THREAD_SLEEP_TIME)
        self._process: Process = process
        self._callback: Callable[[], None] = callback

    def work(self) -> None:
        if not self._process.is_alive():
            logger.error(f"{self._process.name} is not alive.")
            self._callback()
