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

    def is_expired(self) -> bool:
        remaining_time = self.remaining_time()
        if remaining_time is None:
            return False
        return remaining_time <= 0.0

    def is_finite(self) -> bool:
        return self.timeout is not None
