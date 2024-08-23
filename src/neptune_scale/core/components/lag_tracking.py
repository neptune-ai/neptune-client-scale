from __future__ import annotations

__all__ = ("LagTracker",)

from multiprocessing.sharedctypes import Synchronized
from multiprocessing.synchronize import Condition
from time import monotonic
from typing import Callable

from neptune_scale.core.components.abstract import Resource
from neptune_scale.core.components.daemon import Daemon
from neptune_scale.core.components.errors_tracking import ErrorsQueue
from neptune_scale.core.components.operations_queue import OperationsQueue
from neptune_scale.parameters import (
    LAG_TRACKER_THREAD_SLEEP_TIME,
    LAG_TRACKER_TIMEOUT,
)


class LagTracker(Daemon, Resource):
    def __init__(
        self,
        errors_queue: ErrorsQueue,
        operations_queue: OperationsQueue,
        last_ack_timestamp: Synchronized[float],
        last_ack_timestamp_wait: Condition,
        async_lag_threshold: float,
        on_async_lag_callback: Callable[[], None],
    ) -> None:
        super().__init__(name="LagTracker", sleep_time=LAG_TRACKER_THREAD_SLEEP_TIME)

        self._errors_queue: ErrorsQueue = errors_queue
        self._operations_queue: OperationsQueue = operations_queue
        self._last_ack_timestamp: Synchronized[float] = last_ack_timestamp
        self._last_ack_timestamp_wait: Condition = last_ack_timestamp_wait
        self._async_lag_threshold: float = async_lag_threshold
        self._on_async_lag_callback: Callable[[], None] = on_async_lag_callback

        self._callback_triggered: bool = False

    def work(self) -> None:
        with self._last_ack_timestamp_wait:
            self._last_ack_timestamp_wait.wait(timeout=LAG_TRACKER_TIMEOUT)
            last_ack_timestamp = self._last_ack_timestamp.value
            last_put_timestamp = self._operations_queue.last_timestamp

            # No operations were put into the queue
            if last_put_timestamp is None:
                return

            # No operations were processed by server
            if last_ack_timestamp < 0 and not self._callback_triggered:
                if monotonic() - last_put_timestamp > self._async_lag_threshold:
                    self._callback_triggered = True
                    self._on_async_lag_callback()
                    return

                self._callback_triggered = False
            else:
                # Some operations were processed by server
                if last_put_timestamp - last_ack_timestamp > self._async_lag_threshold:
                    if not self._callback_triggered:
                        self._callback_triggered = True
                        self._on_async_lag_callback()
                        return

                    self._callback_triggered = False
