from queue import (
    Empty,
    Queue,
)
from typing import (
    Callable,
    Generic,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

K = TypeVar("K")
V = TypeVar("V")
QueueElement = Tuple[K, V]


class NotSet:
    pass


DEFAULT_TIMEOUT = 1.0


class BatchingQueue(Generic[K, V]):
    """
    Propoerties:
        1. The order of elements in the batch is the same as the order in which they were put.
        2. All values in returned batch have the same key.
        3. Values that evaluate to False after applying is_batchable_fn() are always returned as a single-element batch.
        4. Returned batch is always of size <= batch_size.
    """

    def __init__(
        self,
        batch_size: int,
        is_batchable_fn: Callable[[V], bool],
        block: bool = True,
        timeout: Optional[float] = DEFAULT_TIMEOUT,
    ) -> None:
        self._queue: Queue[QueueElement[K, V]] = Queue(maxsize=-1)  # unbounded size
        self._batch_size = batch_size
        self._is_batchable_fn = is_batchable_fn
        self._placeholder: Optional[QueueElement[K, V]] = None
        self._block = block
        self._timeout = timeout

    def put(self, key: K, value: V) -> None:
        self._queue.put((key, value))

    def _get(self) -> QueueElement[K, V]:
        if self._placeholder is not None:
            placeholder = self._placeholder
            self._placeholder = None
            return placeholder

        return self._queue.get(block=self._block, timeout=self._timeout)

    def stop_blocking(self) -> None:
        self._block = False

    def empty(self) -> bool:
        return self._queue.empty()

    def get_batch(self) -> List[QueueElement[K, V]]:
        batch: List[QueueElement[K, V]] = []
        current_key: Union[Type[NotSet], K] = NotSet
        while len(batch) < self._batch_size:
            try:
                key, value = self._get()  # after this line, the placeholder is None
                if current_key is NotSet:
                    current_key = key
            except Empty:
                if self._block:
                    continue
                else:
                    break

            if not self._is_batchable_fn(value):
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
