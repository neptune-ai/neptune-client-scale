from __future__ import annotations

__all__ = ("LagTracker",)

import time
from collections.abc import Callable

from neptune_scale.sync.errors_tracking import ErrorsQueue
from neptune_scale.sync.parameters import (
    LAG_TRACKER_THREAD_SLEEP_TIME,
    LAG_TRACKER_TIMEOUT,
)
from neptune_scale.sync.sequence_tracker import SequenceTracker
from neptune_scale.util import (
    Daemon,
    SharedFloat,
)


class LagTracker(Daemon):
    def __init__(
        self,
        errors_queue: ErrorsQueue,
        sequence_tracker: SequenceTracker,
        last_ack_timestamp: SharedFloat,
        async_lag_threshold: float,
        on_async_lag_callback: Callable[[], None],
    ) -> None:
        super().__init__(name="LagTracker", sleep_time=LAG_TRACKER_THREAD_SLEEP_TIME)

        self._errors_queue: ErrorsQueue = errors_queue
        self._sequence_tracker: SequenceTracker = sequence_tracker
        self._last_ack_timestamp: SharedFloat = last_ack_timestamp
        self._async_lag_threshold: float = async_lag_threshold
        self._on_async_lag_callback: Callable[[], None] = on_async_lag_callback

        self._callback_triggered: bool = False

    def work(self) -> None:
        with self._last_ack_timestamp:
            self._last_ack_timestamp.wait(timeout=LAG_TRACKER_TIMEOUT)
            last_ack_timestamp = self._last_ack_timestamp.value
            last_queued_timestamp = self._sequence_tracker.last_timestamp

            # No operations were put into the queue
            if last_queued_timestamp is None:
                return

            # No operations were processed by server
            if last_ack_timestamp < 0 and not self._callback_triggered:
                if time.time() - last_queued_timestamp > self._async_lag_threshold:
                    self._callback_triggered = True
                    self._on_async_lag_callback()
                    return

                self._callback_triggered = False
            else:
                # Some operations were processed by server
                if last_queued_timestamp - last_ack_timestamp > self._async_lag_threshold:
                    if not self._callback_triggered:
                        self._callback_triggered = True
                        self._on_async_lag_callback()
                        return

                    self._callback_triggered = False
