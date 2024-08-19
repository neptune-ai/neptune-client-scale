from __future__ import annotations

__all__ = ("ErrorsQueue", "ErrorsMonitor")

import multiprocessing
import queue
from typing import (
    Callable,
    Optional,
)

from neptune_scale.core.components.abstract import Resource
from neptune_scale.core.components.daemon import Daemon
from neptune_scale.core.logger import logger
from neptune_scale.core.process_killer import kill_me
from neptune_scale.exceptions import NeptuneOperationsQueueMaxSizeExceeded
from neptune_scale.parameters import ERRORS_MONITOR_THREAD_SLEEP_TIME


class ErrorsQueue(Resource):
    def __init__(self) -> None:
        self._errors_queue: multiprocessing.Queue[BaseException] = multiprocessing.Queue()

    def put(self, error: BaseException) -> None:
        self._errors_queue.put(error)

    def get(self, block: bool = True, timeout: Optional[float] = None) -> BaseException:
        return self._errors_queue.get(block=block, timeout=timeout)

    def close(self) -> None:
        self._errors_queue.close()
        # This is needed to avoid hanging the main process
        self._errors_queue.cancel_join_thread()


def default_error_callback(error: BaseException) -> None:
    logger.error(error)
    kill_me()


def default_max_queue_size_exceeded_callback(error: BaseException) -> None:
    logger.warning(error)


class ErrorsMonitor(Daemon, Resource):
    def __init__(
        self,
        errors_queue: ErrorsQueue,
        max_queue_size_exceeded_callback: Optional[Callable[[BaseException], None]] = None,
        on_error_callback: Optional[Callable[[BaseException], None]] = None,
    ):
        super().__init__(name="ErrorsMonitor", sleep_time=ERRORS_MONITOR_THREAD_SLEEP_TIME)

        self._errors_queue: ErrorsQueue = errors_queue
        self._max_queue_size_exceeded_callback: Callable[[BaseException], None] = (
            max_queue_size_exceeded_callback or default_max_queue_size_exceeded_callback
        )
        self._on_error_callback: Callable[[BaseException], None] = on_error_callback or default_error_callback

    def get_next(self) -> Optional[BaseException]:
        try:
            return self._errors_queue.get(block=False)
        except queue.Empty:
            return None

    def work(self) -> None:
        while (error := self.get_next()) is not None:
            if isinstance(error, NeptuneOperationsQueueMaxSizeExceeded):
                self._max_queue_size_exceeded_callback(error)
            else:
                self._on_error_callback(error)
