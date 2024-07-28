from __future__ import annotations

__all__ = ("OperationsQueue",)

from multiprocessing import Queue
from time import monotonic
from typing import (
    TYPE_CHECKING,
    Callable,
    NamedTuple,
)

from neptune_scale.core.components.abstract import Resource
from neptune_scale.core.validation import verify_type

if TYPE_CHECKING:
    from threading import RLock

    from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation


class QueueElement(NamedTuple):
    sequence_id: int
    occured_at: float
    operation: bytes


def default_max_size_exceeded_callback(max_size: int, e: BaseException) -> None:
    raise ValueError(f"Queue is full (max size: {max_size})") from e


class OperationsQueue(Resource):
    def __init__(
        self,
        *,
        lock: RLock,
        max_size: int = 0,
        max_size_exceeded_callback: Callable[[int, BaseException], None] | None = None,
    ) -> None:
        verify_type("max_size", max_size, int)

        self._lock: RLock = lock
        self._max_size: int = max_size
        self._max_size_exceeded_callback: Callable[[int, BaseException], None] = (
            max_size_exceeded_callback if max_size_exceeded_callback is not None else default_max_size_exceeded_callback
        )

        self._sequence_id: int = 0
        self._queue: Queue[QueueElement] = Queue(maxsize=max_size)

    def enqueue(self, *, operation: RunOperation) -> None:
        try:
            with self._lock:
                self._queue.put_nowait(QueueElement(self._sequence_id, monotonic(), operation.SerializeToString()))
                self._sequence_id += 1
        except Exception as e:
            self._max_size_exceeded_callback(self._max_size, e)

    def cleanup(self) -> None:
        pass

    def close(self) -> None:
        self._queue.close()
