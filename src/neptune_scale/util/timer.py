import math
import time
from typing import Optional


class Timer:
    def __init__(self, timeout: Optional[float]) -> None:
        self.timeout = timeout
        self.start_time = time.monotonic()

    def remaining_time(self) -> Optional[float]:
        if self.timeout is None:
            return None

        elapsed_time = time.monotonic() - self.start_time
        return max(0.0, self.timeout - elapsed_time)

    def remaining_time_or_inf(self) -> float:
        remaining_time = self.remaining_time()
        if remaining_time is None:
            return math.inf
        return remaining_time

    def is_expired(self) -> bool:
        if self.timeout is None:
            return False

        elapsed_time = time.monotonic() - self.start_time
        return elapsed_time >= self.timeout

    def is_finite(self) -> bool:
        return self.timeout is not None
