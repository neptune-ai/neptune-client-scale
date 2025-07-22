from __future__ import annotations

__all__ = ("ErrorsMonitor",)

import time
from collections.abc import Callable
from typing import Optional

from neptune_scale.exceptions import (
    NeptuneConnectionLostError,
    NeptuneRetryableError,
    NeptuneScaleError,
    NeptuneScaleWarning,
    NeptuneUnexpectedError,
)
from neptune_scale.sync.operations_repository import OperationsRepository
from neptune_scale.sync.parameters import ERRORS_MONITOR_THREAD_SLEEP_TIME
from neptune_scale.util import get_logger
from neptune_scale.util.daemon import Daemon

logger = get_logger()


def default_error_callback(error: BaseException, last_seen_at: Optional[float]) -> None:
    logger.error(error)


def default_network_error_callback(error: BaseException, last_seen_at: Optional[float]) -> None:
    if last_seen_at is None or time.time() - last_seen_at > 5:
        logger.warning(f"A network error occurred: {error}. Retrying...")


def default_warning_callback(error: BaseException, last_seen_at: Optional[float]) -> None:
    logger.warning(error)


class ErrorsMonitor(Daemon):
    def __init__(
        self,
        operations_repository: OperationsRepository,
        on_network_error_callback: Optional[Callable[[BaseException, Optional[float]], None]] = None,
        on_error_callback: Optional[Callable[[BaseException, Optional[float]], None]] = None,
        on_warning_callback: Optional[Callable[[BaseException, Optional[float]], None]] = None,
    ):
        super().__init__(name="ErrorsMonitor", sleep_time=ERRORS_MONITOR_THREAD_SLEEP_TIME)

        self._operations_repository: OperationsRepository = operations_repository
        self._on_network_error_callback: Callable[[BaseException, Optional[float]], None] = (
            on_network_error_callback or default_network_error_callback
        )
        self._on_error_callback: Callable[[BaseException, Optional[float]], None] = (
            on_error_callback or default_error_callback
        )
        self._on_warning_callback: Callable[[BaseException, Optional[float]], None] = (
            on_warning_callback or default_warning_callback
        )
        self._last_raised_timestamps: dict[str, float] = {}

        self._drain_errors()

    def _drain_errors(self) -> None:
        errors_count = self._operations_repository.get_errors_count()
        if errors_count > 0:
            logger.warning(f"Found {errors_count} unprocessed errors from a previous run. Deleting them now.")
            self._operations_repository.delete_all_errors()

    def work(self) -> None:
        while op_errors := self._operations_repository.get_errors(limit=1):
            op_error = op_errors[0]
            last_raised_at = self._last_raised_timestamps.get(op_error.error_type, None)
            self._last_raised_timestamps[op_error.error_type] = time.time()
            error = op_error.deserialize_error()

            try:
                if isinstance(error, NeptuneConnectionLostError):
                    self._on_network_error_callback(error, last_raised_at)
                elif isinstance(error, NeptuneScaleWarning):
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
            finally:
                if op_error.error_id:
                    self._operations_repository.delete_errors([op_error.error_id])
