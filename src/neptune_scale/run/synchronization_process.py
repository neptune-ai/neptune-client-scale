from multiprocessing import Queue
from threading import Thread
from typing import (
    List,
    Tuple,
)

from neptune_scale.queue.batching_queue import BatchingQueue
from neptune_scale.run.operation import (
    Operation,
    is_batchable_fn,
)

BATCH_SIZE = 4


class OperationQueuePopulator(Thread):
    def __init__(
        self,
        batching_queue: BatchingQueue[int, Operation],
        message_queue: Queue,
    ) -> None:
        super().__init__()
        self.batching_queue = batching_queue
        self.message_queue = message_queue

    def run(self) -> None:
        while True:
            _, operation = self.message_queue.get()
            self.batching_queue.put(operation.step, operation)


class OperationQueueProcessor(Thread):
    def __init__(self, batching_queue: BatchingQueue[int, Operation]) -> None:
        super().__init__()
        self.batching_queue = batching_queue

    def _process_batch(self, batch: List[Tuple[int, Operation]]) -> None:
        print(f"Processing batch {batch}\n")

    def run(self) -> None:
        while True:
            batch = self.batching_queue.get_batch()
            self._process_batch(batch)


def synchronization_process_routine(message_queue: Queue) -> None:
    batching_queue = BatchingQueue[int, Operation](
        batch_size=BATCH_SIZE,
        is_batchable_fn=is_batchable_fn,
        timeout=None,
    )
    populator = OperationQueuePopulator(batching_queue, message_queue)
    processor = OperationQueueProcessor(batching_queue)

    populator.start()
    processor.start()
    populator.join()
    processor.join()
