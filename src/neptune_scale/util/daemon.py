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
        INTERRUPTING = 5
        INTERRUPTED = 6
        STOPPED = 7

    def __init__(self, sleep_time: float, name: str) -> None:
        super().__init__(daemon=True, name=name)
        self._sleep_time = sleep_time
        self._state: Daemon.DaemonState = Daemon.DaemonState.INIT
        self._wait_condition = threading.Condition()

    def interrupt(self, work_final_time: bool = False) -> None:
        """
        Stop the thread.

        If last_work is True, the thread will work one more time before stopping.
        """
        logger.debug(f"Interrupting thread {self.name}")
        with self._wait_condition:
            if work_final_time:
                self._state = Daemon.DaemonState.INTERRUPTING
            else:
                self._state = Daemon.DaemonState.INTERRUPTED
            self._wait_condition.notify_all()

    def terminate(self) -> None:
        logger.debug(f"Thread {self} will stop.")
        with self._wait_condition:
            self._state = Daemon.DaemonState.INTERRUPTING
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
                Daemon.DaemonState.INTERRUPTING,
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

                if self._state in (Daemon.DaemonState.WORKING, Daemon.DaemonState.INTERRUPTING):
                    self.work()
                    with self._wait_condition:
                        if self._state == Daemon.DaemonState.INTERRUPTING:
                            self._state = Daemon.DaemonState.INTERRUPTED
                        if self._sleep_time > 0 and self._state == Daemon.DaemonState.WORKING:
                            self._wait_condition.wait(timeout=self._sleep_time)
        finally:
            with self._wait_condition:
                self._state = Daemon.DaemonState.STOPPED
                self._wait_condition.notify_all()

            logger.debug(f"Thread {self.name} is finished.")

    @abc.abstractmethod
    def work(self) -> None: ...
