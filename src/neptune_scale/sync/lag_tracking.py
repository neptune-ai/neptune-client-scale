#
# Copyright (c) 2025, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

__all__ = ("LagTracker",)

from collections.abc import Callable
from time import monotonic

from neptune_scale.sync.errors_tracking import ErrorsQueue
from neptune_scale.sync.operations_queue import OperationsQueue
from neptune_scale.sync.parameters import (
    LAG_TRACKER_THREAD_SLEEP_TIME,
    LAG_TRACKER_TIMEOUT,
)
from neptune_scale.util import (
    Daemon,
    SharedFloat,
)
from neptune_scale.util.abstract import Resource


class LagTracker(Daemon, Resource):
    def __init__(
        self,
        errors_queue: ErrorsQueue,
        operations_queue: OperationsQueue,
        last_ack_timestamp: SharedFloat,
        async_lag_threshold: float,
        on_async_lag_callback: Callable[[], None],
    ) -> None:
        super().__init__(name="LagTracker", sleep_time=LAG_TRACKER_THREAD_SLEEP_TIME)

        self._errors_queue: ErrorsQueue = errors_queue
        self._operations_queue: OperationsQueue = operations_queue
        self._last_ack_timestamp: SharedFloat = last_ack_timestamp
        self._async_lag_threshold: float = async_lag_threshold
        self._on_async_lag_callback: Callable[[], None] = on_async_lag_callback

        self._callback_triggered: bool = False

    def work(self) -> None:
        with self._last_ack_timestamp:
            self._last_ack_timestamp.wait(timeout=LAG_TRACKER_TIMEOUT)
            last_ack_timestamp = self._last_ack_timestamp.value
            last_queued_timestamp = self._operations_queue.last_timestamp

            # No operations were put into the queue
            if last_queued_timestamp is None:
                return

            # No operations were processed by server
            if last_ack_timestamp < 0 and not self._callback_triggered:
                if monotonic() - last_queued_timestamp > self._async_lag_threshold:
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
