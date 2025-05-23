from __future__ import annotations

__all__ = ("ErrorsQueue", "ErrorsMonitor")

import multiprocessing
import queue
import time
from collections.abc import Callable
from multiprocessing.context import BaseContext
from typing import Optional

from neptune_scale.exceptions import (
    NeptuneAsyncLagThresholdExceeded,
    NeptuneConnectionLostError,
    NeptuneOperationsQueueMaxSizeExceeded,
    NeptuneRetryableError,
    NeptuneScaleError,
    NeptuneScaleWarning,
    NeptuneTooManyRequestsResponseError,
    NeptuneUnexpectedError,
)
from neptune_scale.sync.parameters import ERRORS_MONITOR_THREAD_SLEEP_TIME
from neptune_scale.util import get_logger
from neptune_scale.util.daemon import Daemon

logger = get_logger()


class ErrorsQueue:
    def __init__(self, multiprocessing_context: BaseContext) -> None:
        self._errors_queue: multiprocessing.Queue[BaseException] = multiprocessing_context.Queue()

    def put(self, error: BaseException) -> None:
        logger.debug("Put error: %s", type(error))
        self._errors_queue.put(error)

    def get(self, block: bool = True, timeout: Optional[float] = None) -> BaseException:
        return self._errors_queue.get(block=block, timeout=timeout)

    def close(self) -> None:
        self._errors_queue.close()
        # This is needed to avoid hanging the main process
        self._errors_queue.cancel_join_thread()


def default_error_callback(error: BaseException, last_seen_at: Optional[float]) -> None:
    logger.error(error)


def default_network_error_callback(error: BaseException, last_seen_at: Optional[float]) -> None:
    if last_seen_at is None or time.time() - last_seen_at > 5:
        logger.warning(f"A network error occurred: {error}. Retrying...")


def default_max_queue_size_exceeded_callback(error: BaseException, last_raised_at: Optional[float]) -> None:
    if last_raised_at is None or time.time() - last_raised_at > 5:
        logger.warning(f"Pending operations queue size exceeded: {error}. Retrying...")


def default_warning_callback(error: BaseException, last_seen_at: Optional[float]) -> None:
    logger.warning(error)


class ErrorsMonitor(Daemon):
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

        self._last_raised_timestamps: dict[type[BaseException], float] = {}

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
                elif isinstance(error, NeptuneAsyncLagThresholdExceeded):
                    self._on_async_lag_callback()
                elif isinstance(error, NeptuneScaleWarning):
                    self._on_warning_callback(error, last_raised_at)
                elif isinstance(error, NeptuneTooManyRequestsResponseError):
                    self._on_warning_callback(error, last_raised_at)
                elif isinstance(error, NeptuneRetryableError):
                    self._on_warning_callback(error, last_raised_at)
                elif isinstance(error, NeptuneScaleError):
                    self._on_error_callback(error, last_raised_at)
                else:
                    self._on_error_callback(NeptuneUnexpectedError(reason=str(error)), last_raised_at)
            except Exception as e:
                # Don't let user errors kill the process
                logger.error(f"An exception occurred in user callback function: {e}")
