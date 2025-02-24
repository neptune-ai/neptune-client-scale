from __future__ import annotations

__all__ = ("OperationsQueue",)

import math
import os
import queue
from collections.abc import Hashable
from multiprocessing import Queue
from time import monotonic
from typing import (
    TYPE_CHECKING,
    Optional,
)

from neptune_scale.api.validation import verify_type
from neptune_scale.exceptions import NeptuneUnableToLogData
from neptune_scale.sync.parameters import (
    MAX_MULTIPROCESSING_QUEUE_SIZE,
    MAX_QUEUE_ELEMENT_SIZE,
    MAX_QUEUE_SIZE,
)
from neptune_scale.sync.queue_element import SingleOperation
from neptune_scale.util import (
    envs,
    get_logger,
)
from neptune_scale.util.abstract import Resource

if TYPE_CHECKING:
    from threading import RLock

    from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

logger = get_logger()


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
        self._queue: Queue[SingleOperation] = Queue(maxsize=min(MAX_MULTIPROCESSING_QUEUE_SIZE, max_size))
        self._last_successful_put_time = monotonic()

        self._free_queue_slot_timeout = envs.get_int(envs.LOG_MAX_BLOCKING_TIME_SECONDS, None) or math.inf
        if self._free_queue_slot_timeout < 0:
            raise ValueError(f"{envs.LOG_MAX_BLOCKING_TIME_SECONDS} must be a non-negative number.")

        action = os.getenv(envs.LOG_FAILURE_ACTION, "drop")
        if action not in ("drop", "raise"):
            raise ValueError(f"Invalid value '{action}' for {envs.LOG_FAILURE_ACTION}. Must be 'drop' or 'raise'.")

        self._raise_on_enqueue_failure = action == "raise"

    @property
    def queue(self) -> Queue[SingleOperation]:
        return self._queue

    @property
    def last_sequence_id(self) -> int:
        with self._lock:
            return self._sequence_id - 1

    @property
    def last_timestamp(self) -> Optional[float]:
        with self._lock:
            return self._last_timestamp

    def enqueue(self, *, operation: RunOperation, size: Optional[int] = None, key: Hashable = None) -> None:
        try:
            is_metadata_update = operation.HasField("update")
            serialized_operation = operation.SerializeToString()

            if len(serialized_operation) > MAX_QUEUE_ELEMENT_SIZE:
                raise ValueError(f"Operation size exceeds the maximum allowed size ({MAX_QUEUE_ELEMENT_SIZE})")

            with self._lock:
                self._last_timestamp = monotonic()

                item = SingleOperation(
                    sequence_id=self._sequence_id,
                    timestamp=self._last_timestamp,
                    operation=serialized_operation,
                    metadata_size=size,
                    is_batchable=is_metadata_update,
                    batch_key=key,
                )

                # Optimistically put the item without blocking. If the queue is full, we will retry
                # the put with blocking, but only if the last successful put was lest than the `timeout` ago.
                # This way if the sync process is stuck, we will drop operations until we are able to successfully
                # put an item into the queue again after some of the pending items were processed.
                try:
                    self._queue.put_nowait(item)
                    self._last_successful_put_time = monotonic()
                except queue.Full:
                    if monotonic() - self._last_successful_put_time < self._free_queue_slot_timeout:
                        try:
                            self._queue.put(item, block=True, timeout=self._free_queue_slot_timeout)
                            self._last_successful_put_time = monotonic()
                        except queue.Full:
                            self._on_enqueue_failed("Operations queue is full", operation)
                            return
                    else:
                        self._on_enqueue_failed("Operations queue is full", operation)
                        return

                self._sequence_id += 1
        # Pass this through, as it's raised intentionally in _on_enqueue_failed()
        except NeptuneUnableToLogData as e:
            raise e
        except Exception as e:
            self._on_enqueue_failed(reason=str(e), operation=operation)

    def close(self) -> None:
        self._queue.close()
        # This is needed to avoid hanging the main process
        self._queue.cancel_join_thread()

    def _on_enqueue_failed(self, reason: str, operation: RunOperation) -> None:
        if self._raise_on_enqueue_failure:
            raise NeptuneUnableToLogData(reason=reason, operation=str(operation))
        else:
            logger.error(f"Dropping operation due to error: {reason}. Operation: {operation}")
