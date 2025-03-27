from __future__ import annotations

__all__ = "ErrorsHandler"

import multiprocessing
import queue
import time
from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import Callable
from enum import Enum
from typing import Optional

from neptune_scale.exceptions import (
    NeptuneAsyncLagThresholdExceeded,
    NeptuneConnectionLostError,
    NeptuneRetryableError,
    NeptuneScaleError,
    NeptuneScaleWarning,
    NeptuneUnexpectedError,
)
from neptune_scale.sync.parameters import ERRORS_MONITOR_THREAD_SLEEP_TIME
from neptune_scale.util import get_logger
from neptune_scale.util.daemon import Daemon

logger = get_logger()


class ErrorsQueue:
    def __init__(self) -> None:
        self._errors_queue: multiprocessing.Queue[BaseException] = multiprocessing.Queue()

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


def default_warning_callback(error: BaseException, last_seen_at: Optional[float]) -> None:
    logger.warning(error)


def default_async_lag_callback() -> None:
    pass


class ErrorsHandler(ABC):
    @abstractmethod
    def handle(self, error: BaseException) -> None: ...


class CustomErrorsHandler(ErrorsHandler):
    def __init__(
        self,
        on_async_lag_callback: Optional[Callable[[], None]] = None,
        on_network_error_callback: Optional[Callable[[BaseException, Optional[float]], None]] = None,
        on_error_callback: Optional[Callable[[BaseException, Optional[float]], None]] = None,
        on_warning_callback: Optional[Callable[[BaseException, Optional[float]], None]] = None,
    ):
        self._on_async_lag_callback: Callable[[], None] = on_async_lag_callback or default_async_lag_callback
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

    def handle(self, error: BaseException) -> None:
        last_raised_at = self._last_raised_timestamps.get(type(error), None)
        self._last_raised_timestamps[type(error)] = time.time()

        if isinstance(error, NeptuneConnectionLostError):
            self._on_network_error_callback(error, last_raised_at)
        elif isinstance(error, NeptuneAsyncLagThresholdExceeded):
            self._on_async_lag_callback()
        elif isinstance(error, NeptuneScaleWarning):
            self._on_warning_callback(error, last_raised_at)
        elif isinstance(error, NeptuneRetryableError):
            self._on_warning_callback(error, last_raised_at)
        elif isinstance(error, NeptuneScaleError):
            self._on_error_callback(error, last_raised_at)
        else:
            self._on_error_callback(NeptuneUnexpectedError(reason=str(error)), last_raised_at)


class RemoteErrorsHandlerAction(Enum):
    HANDLE = 0
    SEND = 1


class RemoteErrorsHandler(ErrorsHandler):
    def __init__(
        self,
        errors_queue: Optional[ErrorsQueue] = None,
        on_async_lag_action: RemoteErrorsHandlerAction = RemoteErrorsHandlerAction.HANDLE,
        on_network_error_action: RemoteErrorsHandlerAction = RemoteErrorsHandlerAction.HANDLE,
        on_error_action: RemoteErrorsHandlerAction = RemoteErrorsHandlerAction.HANDLE,
        on_warning_action: RemoteErrorsHandlerAction = RemoteErrorsHandlerAction.HANDLE,
    ):
        if errors_queue is None and any(
            action == RemoteErrorsHandlerAction.SEND
            for action in (on_async_lag_action, on_network_error_action, on_error_action, on_warning_action)
        ):
            raise ValueError("Errors queue must be provided if any action is set to SEND")

        self._errors_queue: Optional[ErrorsQueue] = errors_queue
        self._on_async_lag_action: RemoteErrorsHandlerAction = on_async_lag_action
        self._on_network_error_action: RemoteErrorsHandlerAction = on_network_error_action
        self._on_error_action: RemoteErrorsHandlerAction = on_error_action
        self._on_warning_action: RemoteErrorsHandlerAction = on_warning_action

        self._last_raised_timestamps: dict[type[BaseException], float] = {}

    def handle(self, error: BaseException) -> None:
        last_raised_at = self._last_raised_timestamps.get(type(error), None)
        self._last_raised_timestamps[type(error)] = time.time()

        if isinstance(error, NeptuneConnectionLostError):
            if self._on_network_error_action == RemoteErrorsHandlerAction.HANDLE:
                default_network_error_callback(error, last_raised_at)
            else:
                self._errors_queue.put(error)  # type: ignore
        elif isinstance(error, NeptuneAsyncLagThresholdExceeded):
            if self._on_async_lag_action == RemoteErrorsHandlerAction.HANDLE:
                default_async_lag_callback()
            else:
                self._errors_queue.put(error)  # type: ignore
        elif isinstance(error, NeptuneScaleWarning):
            if self._on_warning_action == RemoteErrorsHandlerAction.HANDLE:
                default_warning_callback(error, last_raised_at)
            else:
                self._errors_queue.put(error)  # type: ignore
        elif isinstance(error, NeptuneRetryableError):
            if self._on_warning_action == RemoteErrorsHandlerAction.HANDLE:
                default_warning_callback(error, last_raised_at)
            else:
                self._errors_queue.put(error)  # type: ignore
        elif isinstance(error, NeptuneScaleError):
            if self._on_error_action == RemoteErrorsHandlerAction.HANDLE:
                default_error_callback(error, last_raised_at)
            else:
                self._errors_queue.put(error)  # type: ignore
        else:
            if self._on_error_action == RemoteErrorsHandlerAction.HANDLE:
                default_error_callback(NeptuneUnexpectedError(reason=str(error)), last_raised_at)
            else:
                self._errors_queue.put(NeptuneUnexpectedError(reason=str(error)))  # type: ignore


class ErrorsMonitor(Daemon):
    def __init__(
        self,
        errors_queue: ErrorsQueue,
        errors_handler: ErrorsHandler,
    ):
        super().__init__(name="ErrorsMonitor", sleep_time=ERRORS_MONITOR_THREAD_SLEEP_TIME)

        self._errors_queue: ErrorsQueue = errors_queue
        self._errors_handler: ErrorsHandler = errors_handler

    def get_next(self) -> Optional[BaseException]:
        try:
            return self._errors_queue.get(block=False)
        except queue.Empty:
            return None

    def work(self) -> None:
        while (error := self.get_next()) is not None:
            try:
                self._errors_handler.handle(error)
            except Exception as e:
                # Don't let user errors kill the process
                logger.error(f"An exception occurred in user callback function: {e}")
