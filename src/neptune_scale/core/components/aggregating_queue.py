from __future__ import annotations

__all__ = ("AggregatingQueue",)

from queue import (
    Empty,
    Queue,
)
from threading import RLock
from typing import Optional

from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale.core.components.abstract import Resource
from neptune_scale.core.components.queue_element import BatchedOperations
from neptune_scale.parameters import MAX_QUEUE_ELEMENT_SIZE


class AggregatingQueue(Resource):
    def __init__(
        self,
        max_queue_size: int,
        max_elements_in_batch: int = 1,  # TODO: Restore default value to MAX_BATCH_SIZE
        max_queue_element_size: int = MAX_QUEUE_ELEMENT_SIZE,
    ) -> None:
        self._max_queue_size = max_queue_size
        self._max_elements_in_batch = max_elements_in_batch
        self._max_queue_element_size = max_queue_element_size

        self._queue: Queue[BatchedOperations] = Queue(maxsize=max_queue_size)
        self._lock: RLock = RLock()
        self._latest_unprocessed: Optional[BatchedOperations] = None

    @property
    def maxsize(self) -> int:
        return self._max_queue_size

    def put_nowait(self, element: BatchedOperations) -> None:
        with self._lock:
            self._queue.put_nowait(element)

    def _get_next(self) -> Optional[BatchedOperations]:
        # We can assume that each of queue elements are less than MAX_QUEUE_ELEMENT_SIZE
        # We can assume that every queue element has the same project, run id and family
        if self._latest_unprocessed is not None:
            return self._latest_unprocessed

        try:
            self._latest_unprocessed = self._queue.get_nowait()
            return self._latest_unprocessed
        except Empty:
            return None

    def commit(self) -> None:
        self._latest_unprocessed = None

    def get_nowait(self) -> BatchedOperations:
        with self._lock:
            elements_in_batch: int = 0
            batch: Optional[RunOperation] = None
            batch_sequence_id: Optional[int] = None
            batch_timestamp: Optional[float] = None

            while (element := self._get_next()) is not None:
                elements_in_batch += 1

                if elements_in_batch > self._max_elements_in_batch:
                    break

                if batch is None:
                    batch = RunOperation()
                    batch.ParseFromString(element.operation)
                    batch_sequence_id, batch_timestamp = element.sequence_id, element.timestamp
                    self.commit()
                else:
                    # TODO: Check if steps and timestamps are the same
                    # TODO: Merge if size allows
                    # TODO: Update batch sequence_id and timestamp
                    pass
                    break

            if batch is None:
                raise Empty

            assert batch_sequence_id is not None  # mypy
            assert batch_timestamp is not None  # mypy

            return BatchedOperations(
                sequence_id=batch_sequence_id,
                timestamp=batch_timestamp,
                operation=batch.SerializeToString(),
            )
