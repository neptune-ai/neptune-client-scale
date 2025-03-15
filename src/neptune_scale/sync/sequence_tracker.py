from __future__ import annotations

__all__ = ("SequenceTracker",)

import threading
import time
from typing import Optional

from neptune_scale.sync.operations_repository import SequenceId


class SequenceTracker:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._last_sequence_id: SequenceId = SequenceId(-1)
        self._last_timestamp: Optional[float] = None

    @property
    def last_sequence_id(self) -> SequenceId:
        with self._lock:
            return self._last_sequence_id

    @property
    def last_timestamp(self) -> Optional[float]:
        with self._lock:
            return self._last_timestamp

    def update_sequence_id(self, sequence_id: SequenceId) -> None:
        with self._lock:
            # Use max to ensure that the sequence ID is always increasing
            self._last_sequence_id = max(self._last_sequence_id, sequence_id)
            self._last_timestamp = time.time()
