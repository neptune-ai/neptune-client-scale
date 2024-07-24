from datetime import datetime
from multiprocessing import (
    Process,
    Queue,
)
from typing import Tuple

from neptune_scale.run.operation import Operation
from neptune_scale.run.synchronization_process import synchronization_process_routine


class Run:
    def __init__(self) -> None:
        self.message_queue: Queue[Tuple[datetime, Operation]] = Queue()
        self.process = Process(target=synchronization_process_routine, args=(self.message_queue,))
        self.process.start()

    def log(self, datetime: datetime, operation: Operation) -> None:
        self.message_queue.put((datetime, operation))


if __name__ == "__main__":
    run = Run()
    some_date = datetime.now()

    while True:
        value = int(input("Enter a integer value: "))
        step = int(input("Enter a operation step: "))
        is_batchable = input("Is batchable? (y/n): ")
        should_batch = True if is_batchable == "y" else False

        operation = Operation(should_batch=should_batch, value=value, step=step)
        run.log(some_date, operation)
