__all__ = ["Daemon"]

import abc
import threading
from typing import Optional

from neptune_scale.util.logger import get_logger

logger = get_logger()


class Daemon(threading.Thread):
    def __init__(self, sleep_time: float, name: str) -> None:
        super().__init__(daemon=True, name=name)
        self._sleep_time = sleep_time
        self._wait_condition = threading.Condition()
        self._remaining_iterations: Optional[int] = None

    def interrupt(self, remaining_iterations: int = 0) -> None:
        """
        Stop the thread.

        If remaining_iterations is not 0, the thread will work remaining_iterations times before stopping.
        """
        if remaining_iterations < 0:
            raise RuntimeError("Remaining iterations must be a non-negative integer.")

        logger.debug(f"Interrupting thread {self.name}")

        with self._wait_condition:
            self._remaining_iterations = remaining_iterations
            self._wait_condition.notify_all()

    def run(self) -> None:
        try:
            while True:
                with self._wait_condition:
                    do_work = self._remaining_iterations is None or self._remaining_iterations > 0
                    if self._remaining_iterations is not None:
                        self._remaining_iterations -= 1

                if do_work:
                    self.work()

                    with self._wait_condition:
                        if self._remaining_iterations is None or self._remaining_iterations > 0:
                            self._wait_condition.wait(timeout=self._sleep_time)
                else:
                    self.close()
                    break
        finally:
            logger.debug(f"Thread {self.name} is finished.")

    def close(self) -> None:
        """
        This method is called after the last iteration of the thread.
        It is only called if the last work() iteration was successful (has not raised an exception).
        """
        pass

    @abc.abstractmethod
    def work(self) -> None: ...
