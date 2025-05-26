from __future__ import annotations

__all__ = ("ProcessSupervisor",)

from collections.abc import Callable
from multiprocessing.process import BaseProcess
from typing import Optional

from neptune_scale.sync.parameters import PROCESS_SUPERVISOR_THREAD_SLEEP_TIME
from neptune_scale.util import (
    Daemon,
    get_logger,
)

logger = get_logger()


class ProcessSupervisor(Daemon):
    def __init__(self, process: BaseProcess, callback: Callable[[], None]) -> None:
        super().__init__(name="ProcessSupervisor", sleep_time=PROCESS_SUPERVISOR_THREAD_SLEEP_TIME)
        self._process: BaseProcess = process
        self._callback: Callable[[], None] = callback
        self._last_is_alive: Optional[bool] = None

    def work(self) -> None:
        is_alive = self._process.is_alive()

        if self._last_is_alive and not is_alive:
            logger.error(f"{self._process.name} died.")
            self._callback()

        self._last_is_alive = is_alive
