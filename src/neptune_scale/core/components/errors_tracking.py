from __future__ import annotations

__all__ = ("ErrorsQueue", "ErrorsMonitor")

import multiprocessing
import queue
import time
from typing import (
    Callable,
    Optional,
    Type,
)

from neptune_scale.core.components.abstract import Resource
from neptune_scale.core.components.daemon import Daemon
from neptune_scale.core.logger import logger
from neptune_scale.core.process_killer import kill_me
from neptune_scale.exceptions import (
    NeptuneAsyncLagThresholdExceeded,
    NeptuneConnectionLostError,
    NeptuneOperationsQueueMaxSizeExceeded,
    NeptuneScaleError,
    NeptuneScaleWarning,
    NeptuneUnexpectedError,
)
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


def default_error_callback(error: BaseException, last_seen_at: Optional[float]) -> None:
    logger.error(error)
    kill_me()


def default_network_error_callback(error: BaseException, last_seen_at: Optional[float]) -> None:
    logger.warning("Experiencing network issues. Retrying...")


def default_max_queue_size_exceeded_callback(error: BaseException, last_raised_at: Optional[float]) -> None:
    if last_raised_at is None or time.time() - last_raised_at > 5:
        logger.warning(error)


def default_warning_callback(error: BaseException, last_seen_at: Optional[float]) -> None:
    logger.warning(error)


class ErrorsMonitor(Daemon, Resource):
    def __init__(
        self,
        errors_queue: ErrorsQueue,
        on_queue_full_callback: Optional[Callable[[BaseException, Optional[float]], None]] = None,
        on_async_lag_callback: Optional[Callable[[], None]] = None,
        on_network_error_callback: Optional[Callable[[BaseException, Optional[float]], None]] = None,
        on_error_callback: Optional[Callable[[BaseException, Optional[float]], None]] = None,
        on_warning_callback: Optional[Callable[[BaseException, Optional[float]], None]] = None,
    ):
        super().__init__(name="ErrorsMonitor", sleep_time=ERRORS_MONITOR_THREAD_SLEEP_TIME)

        self._errors_queue: ErrorsQueue = errors_queue
        self._on_queue_full_callback: Callable[[BaseException, Optional[float]], None] = (
            on_queue_full_callback or default_max_queue_size_exceeded_callback
        )
        self._on_async_lag_callback: Callable[[], None] = on_async_lag_callback or (lambda: None)
        self._on_network_error_callback: Callable[[BaseException, Optional[float]], None] = (
            on_network_error_callback or default_network_error_callback
        )
        self._on_error_callback: Callable[[BaseException, Optional[float]], None] = (
            on_error_callback or default_error_callback
        )
        self._on_warning_callback: Callable[[BaseException, Optional[float]], None] = (
            on_warning_callback or default_warning_callback
        )

        self._last_raised_timestamps: dict[Type[BaseException], float] = {}

    def get_next(self) -> Optional[BaseException]:
        try:
            return self._errors_queue.get(block=False)
        except queue.Empty:
            return None

    def work(self) -> None:
        while (error := self.get_next()) is not None:
            last_raised_at = self._last_raised_timestamps.get(type(error), None)
            self._last_raised_timestamps[type(error)] = time.time()

            try:
                if isinstance(error, NeptuneOperationsQueueMaxSizeExceeded):
                    self._on_queue_full_callback(error, last_raised_at)
                elif isinstance(error, NeptuneConnectionLostError):
                    self._on_network_error_callback(error, last_raised_at)
                elif isinstance(error, NeptuneScaleWarning):
                    self._on_warning_callback(error, last_raised_at)
                elif isinstance(error, NeptuneScaleError):
                    self._on_error_callback(error, last_raised_at)
                elif isinstance(error, NeptuneAsyncLagThresholdExceeded):
                    self._on_async_lag_callback()
                else:
                    self._on_error_callback(NeptuneUnexpectedError(reason=str(type(error))), last_raised_at)
            except Exception as e:
                # Don't let user errors kill the process
                logger.error(f"An exception occurred in user callback function: {e}")
