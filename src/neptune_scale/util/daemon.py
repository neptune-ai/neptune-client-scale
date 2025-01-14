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

__all__ = ["Daemon"]

import abc
import threading
from enum import Enum

from neptune_scale.util.logger import get_logger

logger = get_logger()


class Daemon(threading.Thread):
    class DaemonState(Enum):
        INIT = 1
        WORKING = 2
        PAUSING = 3
        PAUSED = 4
        INTERRUPTED = 5
        STOPPED = 6

    def __init__(self, sleep_time: float, name: str) -> None:
        super().__init__(daemon=True, name=name)
        self._sleep_time = sleep_time
        self._state: Daemon.DaemonState = Daemon.DaemonState.INIT
        self._wait_condition = threading.Condition()

    def interrupt(self) -> None:
        logger.debug(f"Thread {self} interrupted.")
        with self._wait_condition:
            self._state = Daemon.DaemonState.INTERRUPTED
            self._wait_condition.notify_all()

    def pause(self) -> None:
        with self._wait_condition:
            if self._state != Daemon.DaemonState.PAUSED:
                if not self._is_interrupted():
                    self._state = Daemon.DaemonState.PAUSING
                self._wait_condition.notify_all()
                self._wait_condition.wait_for(lambda: self._state != Daemon.DaemonState.PAUSING)

    def resume(self) -> None:
        with self._wait_condition:
            if not self._is_interrupted():
                self._state = Daemon.DaemonState.WORKING
            self._wait_condition.notify_all()

    def wake_up(self) -> None:
        with self._wait_condition:
            self._wait_condition.notify_all()

    def disable_sleep(self) -> None:
        self._sleep_time = 0

    def is_running(self) -> bool:
        with self._wait_condition:
            return self._state in (
                Daemon.DaemonState.WORKING,
                Daemon.DaemonState.PAUSING,
                Daemon.DaemonState.PAUSED,
            )

    def _is_interrupted(self) -> bool:
        with self._wait_condition:
            return self._state in (Daemon.DaemonState.INTERRUPTED, Daemon.DaemonState.STOPPED)

    def run(self) -> None:
        with self._wait_condition:
            if not self._is_interrupted():
                self._state = Daemon.DaemonState.WORKING
        try:
            while not self._is_interrupted():
                with self._wait_condition:
                    if self._state == Daemon.DaemonState.PAUSING:
                        self._state = Daemon.DaemonState.PAUSED
                        self._wait_condition.notify_all()
                        self._wait_condition.wait_for(lambda: self._state != Daemon.DaemonState.PAUSED)

                if self._state == Daemon.DaemonState.WORKING:
                    self.work()
                    with self._wait_condition:
                        if self._sleep_time > 0 and self._state == Daemon.DaemonState.WORKING:
                            self._wait_condition.wait(timeout=self._sleep_time)
        finally:
            with self._wait_condition:
                self._state = Daemon.DaemonState.STOPPED
                self._wait_condition.notify_all()

            logger.debug(f"Thread {self} is finished.")

    @abc.abstractmethod
    def work(self) -> None: ...
