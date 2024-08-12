__all__ = ("ErrorsMonitor",)

import logging
import queue
from typing import Callable

from neptune_scale.core.components.abstract import Resource
from neptune_scale.core.components.daemon import Daemon
from neptune_scale.core.components.errors_queue import ErrorsQueue

logger = logging.getLogger("neptune")
logger.setLevel(level=logging.INFO)


def on_error(error: BaseException) -> None:
    logger.error(error)


class ErrorsMonitor(Daemon, Resource):
    def __init__(
        self,
        errors_queue: ErrorsQueue,
        on_error_callback: Callable[[BaseException], None] = on_error,
    ):
        super().__init__(name="ErrorsMonitor", sleep_time=2)
        self._errors_queue = errors_queue
        self._on_error_callback = on_error_callback

    def work(self) -> None:
        try:
            error = self._errors_queue.get(block=False)
            if error is not None:
                self._on_error_callback(error)
        except KeyboardInterrupt:
            with self._wait_condition:
                self._wait_condition.notify_all()
            raise
        except queue.Empty:
            pass

    def cleanup(self) -> None:
        pass

    def close(self) -> None:
        self.interrupt()
        self.join(timeout=10)
