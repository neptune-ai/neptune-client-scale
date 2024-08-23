from __future__ import annotations

__all__ = ("OperationsQueue",)

from multiprocessing import Queue
from time import monotonic
from typing import (
    TYPE_CHECKING,
    Optional,
)

from neptune_scale.core.components.abstract import Resource
from neptune_scale.core.components.queue_element import QueueElement
from neptune_scale.core.logger import logger
from neptune_scale.core.validation import verify_type
from neptune_scale.parameters import (
    MAX_MULTIPROCESSING_QUEUE_SIZE,
    MAX_QUEUE_ELEMENT_SIZE,
    MAX_QUEUE_SIZE,
)

if TYPE_CHECKING:
    from threading import RLock

    from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation


class OperationsQueue(Resource):
    def __init__(
        self,
        *,
        lock: RLock,
        max_size: int = MAX_QUEUE_SIZE,
    ) -> None:
        verify_type("max_size", max_size, int)

        self._lock: RLock = lock
        self._max_size: int = max_size

        self._sequence_id: int = 0
        self._last_timestamp: Optional[float] = None
        self._queue: Queue[QueueElement] = Queue(maxsize=min(MAX_MULTIPROCESSING_QUEUE_SIZE, max_size))

    @property
    def queue(self) -> Queue[QueueElement]:
        return self._queue

    @property
    def last_sequence_id(self) -> int:
        with self._lock:
            return self._sequence_id - 1

    @property
    def last_timestamp(self) -> Optional[float]:
        with self._lock:
            return self._last_timestamp

    def enqueue(self, *, operation: RunOperation) -> None:
        try:
            serialized_operation = operation.SerializeToString()

            if len(serialized_operation) > MAX_QUEUE_ELEMENT_SIZE:
                raise ValueError(f"Operation size exceeds the maximum allowed size ({MAX_QUEUE_ELEMENT_SIZE})")

            with self._lock:
                self._last_timestamp = monotonic()
                # TODO: should we not block here, and just call the error callback if we were to block?
                self._queue.put(
                    QueueElement(self._sequence_id, self._last_timestamp, serialized_operation),
                    block=True,
                    timeout=None,
                )
                self._sequence_id += 1
        except Exception as e:
            logger.error("Failed to enqueue operation: %s %s", e, operation)
            raise e

    def close(self) -> None:
        self._queue.close()
        # This is needed to avoid hanging the main process
        self._queue.cancel_join_thread()
