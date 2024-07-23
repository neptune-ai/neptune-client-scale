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

K = TypeVar("K")
V = TypeVar("V")

Timestamp = NewType("Timestamp", float)
QueueElement = Tuple[Optional[K], V]


class NotSet:
    pass


BLOCK_TIMEOUT = 1.0


class OperationQueue(Generic[K, V]):
    """
    Propoerties:
        1. The order of elements in the batch is the same as the order in which they were put.
        2. All values in returned batch have the same key.
        3. Value put with key=None is always returned as a single-element batch.
        4. Returned batch is always of size <= batch_size.
        5. get_batch() blocks until the biggest possible batch is ready to be returned unless stop_blocking() was called.
    """

    def __init__(self, batch_size: int) -> None:
        self._queue: Queue[QueueElement[K, V]] = Queue(maxsize=-1)  # unbounded size
        self._batch_size = batch_size
        self._placeholder: Optional[QueueElement[K, V]] = None
        self._stop_blocking = False

    def put(self, key: Optional[K], value: V) -> None:
        self._queue.put((key, value))

    def _get(self) -> QueueElement[K, V]:
        if self._placeholder is not None:
            placeholder = self._placeholder
            self._placeholder = None
            return placeholder

        return self._queue.get(block=True, timeout=BLOCK_TIMEOUT)

    def stop_blocking(self) -> None:
        self._stop_blocking = True

    def get_batch(self) -> List[QueueElement[K, V]]:
        batch: List[QueueElement[K, V]] = []
        current_key: Union[Type[NotSet], None, K] = NotSet
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
