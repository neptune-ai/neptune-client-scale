from __future__ import annotations

__all__ = ("LagTracker",)

import time
from collections.abc import Callable

from neptune_scale.sync.operations_repository import OperationsRepository
from neptune_scale.sync.parameters import LAG_TRACKER_THREAD_SLEEP_TIME
from neptune_scale.util import Daemon


class LagTracker(Daemon):
    def __init__(
        self,
        operations_repository: OperationsRepository,
        async_lag_threshold: float,
        on_async_lag_callback: Callable[[], None],
    ) -> None:
        super().__init__(name="LagTracker", sleep_time=LAG_TRACKER_THREAD_SLEEP_TIME)

        self._operations_repository = operations_repository
        self._async_lag_threshold: float = async_lag_threshold
        self._on_async_lag_callback: Callable[[], None] = on_async_lag_callback

        self._callback_triggered: bool = False

    def work(self) -> None:
        oldest_queued_timestamp = self._operations_repository.get_operations_min_timestamp()
        current_timestamp = time.time()

        if (
            oldest_queued_timestamp is not None
            and current_timestamp - oldest_queued_timestamp.timestamp() > self._async_lag_threshold
        ):
            if not self._callback_triggered:
                self._on_async_lag_callback()
                self._callback_triggered = True
        else:
            self._callback_triggered = False
