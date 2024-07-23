from queue import (
    Empty,
    Queue,
)
from typing import (
    Generic,
    List,
    NewType,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

T = TypeVar("T")
Timestamp = NewType("Timestamp", float)
TIMEOUT = 1.0

QueueElement = Tuple[Optional[Timestamp], T]


class NotSet:
    pass


class OperationQueue(Generic[T]):
    def __init__(self, batch_size: int) -> None:
        self._queue: Queue[QueueElement[T]] = Queue(maxsize=-1)
        self._batch_size = batch_size
        self._placeholder: Optional[QueueElement[T]] = None
        self._stop_blocking = False

    def put(self, key: Optional[Timestamp], value: T) -> None:
        self._queue.put((key, value))

    def _get(self) -> QueueElement[T]:
        if self._placeholder is not None:
            placeholder = self._placeholder
            self._placeholder = None
            return placeholder

        return self._queue.get(block=True, timeout=TIMEOUT)

    def stop_blocking(self) -> None:
        self._stop_blocking = True

    def get_batch(self) -> List[QueueElement[T]]:
        batch: List[QueueElement[T]] = []
        current_key: Union[Type[NotSet], None, Timestamp] = NotSet
        while len(batch) < self._batch_size:
            try:
                key, value = self._get()  # after this line, the placeholder is None
                if current_key is NotSet:
                    current_key = key
            except Empty:
                if self._stop_blocking:
                    break
                else:
                    continue

            if key is None:
                current_key = NotSet
                if not batch:
                    return [(key, value)]
                else:
                    self._placeholder = (key, value)
                return batch
            elif key != current_key:
                self._placeholder = (key, value)
                current_key = key
                return batch
            else:
                batch.append((key, value))

        return batch
