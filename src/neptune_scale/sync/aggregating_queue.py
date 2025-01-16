#
# Copyright (c) 2025, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

__all__ = ("AggregatingQueue",)

import time
from queue import (
    Empty,
    Queue,
)
from typing import Optional

from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale.sync.parameters import (
    BATCH_WAIT_TIME_SECONDS,
    MAX_BATCH_SIZE,
    MAX_QUEUE_ELEMENT_SIZE,
)
from neptune_scale.sync.queue_element import (
    BatchedOperations,
    SingleOperation,
)
from neptune_scale.util import get_logger
from neptune_scale.util.abstract import Resource

logger = get_logger()


class AggregatingQueue(Resource):
    def __init__(
        self,
        max_queue_size: int,
        max_elements_in_batch: int = MAX_BATCH_SIZE,
        max_queue_element_size: int = MAX_QUEUE_ELEMENT_SIZE,
        wait_time: float = BATCH_WAIT_TIME_SECONDS,
    ) -> None:
        self._max_queue_size = max_queue_size
        self._max_elements_in_batch = max_elements_in_batch
        self._max_queue_element_size = max_queue_element_size
        self._wait_time = wait_time

        self._queue: Queue[SingleOperation] = Queue(maxsize=max_queue_size)
        self._latest_unprocessed: Optional[SingleOperation] = None

    @property
    def maxsize(self) -> int:
        return self._max_queue_size

    def put_nowait(self, element: SingleOperation) -> None:
        self._queue.put_nowait(element)

    def _get_next(self, timeout: float) -> Optional[SingleOperation]:
        # We can assume that each of queue elements are less than MAX_QUEUE_ELEMENT_SIZE because of MetadataSplitter.
        # We can assume that every queue element has the same project, run id and family
        if self._latest_unprocessed is not None:
            return self._latest_unprocessed

        try:
            self._latest_unprocessed = self._queue.get(timeout=timeout)
            return self._latest_unprocessed
        except Empty:
            return None

    def commit(self) -> None:
        self._latest_unprocessed = None

    def get(self) -> BatchedOperations:
        start = time.monotonic()

        batch_operations: list[RunOperation] = []
        batch_sequence_id: Optional[int] = None
        batch_timestamp: Optional[float] = None

        batch_bytes: int = 0
        elements_in_batch: int = 0
        wait_remaining = self._wait_time

        # Pull operations off the queue until we either reach the maximum size, or
        # the specified wait time has passed. This way we maximize the potential batch size.
        while True:
            t0 = time.monotonic()

            if elements_in_batch >= self._max_elements_in_batch:
                logger.debug("Batch closed due to limit of elements in batch %s", elements_in_batch)
                break

            element = self._get_next(wait_remaining)
            if element is None:
                break

            if not batch_operations:
                new_operation = RunOperation()
                new_operation.ParseFromString(element.operation)
                batch_operations.append(new_operation)
                batch_bytes += len(element.operation)
            else:
                if not element.is_batchable:
                    logger.debug("Batch closed due to next operation not being batchable")
                    break

                assert element.metadata_size is not None  # mypy, metadata update always has metadata size

                if batch_bytes + element.metadata_size > self._max_queue_element_size:
                    logger.debug("Batch closed due to size limit %s", batch_bytes + element.metadata_size)
                    break

                new_operation = RunOperation()
                new_operation.ParseFromString(element.operation)
                batch_operations.append(new_operation)
                batch_bytes += element.metadata_size

            batch_sequence_id = element.sequence_id
            batch_timestamp = element.timestamp

            elements_in_batch += 1

            self.commit()

            if not element.is_batchable:
                logger.debug("Batch closed due to the first element not being batchable")
                break

            t1 = time.monotonic()
            wait_remaining -= t1 - t0

            if wait_remaining <= 0:
                logger.debug("Batch closed due to wait time")
                break

        if not batch_operations:
            logger.debug(f"Batch is empty after {self._wait_time} seconds of waiting.")
            raise Empty

        assert batch_sequence_id is not None  # mypy
        assert batch_timestamp is not None  # mypy

        logger.debug(
            "Batched %d operations. Total size %d. Total time %f",
            elements_in_batch,
            batch_bytes,
            time.monotonic() - start,
        )

        batch = create_run_batch(batch_operations)

        return BatchedOperations(
            sequence_id=batch_sequence_id,
            timestamp=batch_timestamp,
            operation=batch.SerializeToString(),
        )


def create_run_batch(operations: list[RunOperation]) -> RunOperation:
    if not operations:
        raise Empty

    if len(operations) == 1:
        return operations[0]

    head = operations[0]
    batch = RunOperation()
    batch.project = head.project
    batch.run_id = head.run_id
    batch.create_missing_project = head.create_missing_project
    batch.api_key = head.api_key

    for operation in operations:
        operation_type = operation.WhichOneof("operation")
        if operation_type == "update":
            batch.update_batch.snapshots.append(operation.update)
        else:
            raise ValueError("Cannot batch operation of type %s", operation_type)

    return batch
