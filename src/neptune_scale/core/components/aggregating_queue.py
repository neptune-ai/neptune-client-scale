from __future__ import annotations

__all__ = ("AggregatingQueue",)

from queue import Queue
from threading import RLock

from neptune_scale.core.components.abstract import Resource
from neptune_scale.core.components.queue_element import QueueElement
from neptune_scale.parameters import (
    MAX_BATCH_SIZE,
    MAX_QUEUE_ELEMENT_SIZE,
)


class AggregatingQueue(Resource):
    def __init__(
        self,
        max_queue_size: int,
        max_batch_size: int = MAX_BATCH_SIZE,
        max_queue_element_size: int = MAX_QUEUE_ELEMENT_SIZE,
    ) -> None:
        self._max_queue_size = max_queue_size
        self._max_batch_size = max_batch_size
        self._max_queue_element_size = max_queue_element_size

        self._queue: Queue[QueueElement] = Queue(maxsize=max_queue_size)
        self._lock: RLock = RLock()

    @property
    def maxsize(self) -> int:
        return self._max_queue_size

    def put_nowait(self, element: QueueElement) -> None:
        with self._lock:
            self._queue.put_nowait(element)

    def get_nowait(self) -> QueueElement:
        with self._lock:
            return self._queue.get_nowait()
