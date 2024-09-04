from __future__ import annotations

__all__ = ("AggregatingQueue",)

from queue import Queue

from neptune_scale.core.components.abstract import Resource
from neptune_scale.core.components.queue_element import QueueElement


class AggregatingQueue(Resource):
    def __init__(self, max_queue_size: int) -> None:
        self._max_queue_size = max_queue_size

        self._queue: Queue[QueueElement] = Queue(maxsize=max_queue_size)

    @property
    def maxsize(self) -> int:
        return self._max_queue_size

    def put_nowait(self, element: QueueElement) -> None:
        self._queue.put_nowait(element)

    def get_nowait(self) -> QueueElement:
        return self._queue.get_nowait()
