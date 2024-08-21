from dataclasses import dataclass
from queue import (
    Empty,
    Queue,
)
from typing import (
    Any,
    Callable,
    Optional,
    Tuple,
    Type,
    Union,
)

from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale.core.components.queue_element import QueueElement


class NotSet:
    pass


@dataclass
class AggregatingQueueElement:
    sequence_id: int
    timestamp: float
    key: Optional[Tuple[int, int]]
    operation: RunOperation


def _cast_to_queue_element(
    fn: Callable[..., Optional[AggregatingQueueElement]],
) -> Callable[..., Optional[QueueElement]]:
    def wrapper(*args: Any, **kwargs: Any) -> Optional[QueueElement]:
        result = fn(*args, **kwargs)
        if result is None:
            return None
        return QueueElement(
            sequence_id=result.sequence_id,
            timestamp=result.timestamp,
            operation=result.operation.SerializeToString(),
        )

    return wrapper


DEFAULT_TIMEOUT = 1.0


class AggregatingQueue:
    """
    Propoerties:
        1. The order of elements in the batch is the same as the order in which they were put.
        2. All values in returned batch have the same key.
        3. Values that evaluate to False after applying is_batchable_fn() are always returned as a single-element batch.
        4. Returned batch is always of size <= batch_size.
    """

    def __init__(
        self,
        max_batch_size: int,
        queue_size: Optional[int] = None,
        block: bool = True,
        batch_max_bytes: Optional[int] = None,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        self._queue: Queue[AggregatingQueueElement] = Queue(
            maxsize=queue_size if queue_size is not None else -1
        )  # unbounded size
        self._max_batch_size = max_batch_size
        self._placeholder: Optional[AggregatingQueueElement] = None
        self._block = block
        self._timeout = timeout
        self._batch_max_bytes = batch_max_bytes if batch_max_bytes is not None else float("inf")

    def put(self, element: QueueElement) -> None:
        operation = RunOperation()
        operation.ParseFromString(element.operation)
        key = self._make_key(operation)

        self._queue.put(
            AggregatingQueueElement(
                sequence_id=element.sequence_id,
                timestamp=element.timestamp,
                key=key,
                operation=operation,
            )
        )

    def _get(self) -> AggregatingQueueElement:
        if self._placeholder is not None:
            placeholder = self._placeholder
            self._placeholder = None
            return placeholder

        return self._queue.get(block=self._block, timeout=self._timeout)

    def _should_aggregate(self, op: RunOperation) -> bool:
        return op.HasField("update")

    def _make_key(self, value: RunOperation) -> Tuple[int, int]:
        if self._should_aggregate(value):
            step = value.update.step
            if step is not None:
                return 0, hash((value.update.step.whole, value.update.step.micro))
            else:
                return 1, 0
        else:
            return 2, 0

    def _merge_element(self, old: AggregatingQueueElement, new: AggregatingQueueElement) -> AggregatingQueueElement:
        if old.key != new.key:
            raise ValueError("Cannot merge elements with different keys")

        if not old.operation.HasField("update") or not new.operation.HasField("update"):
            raise ValueError("Cannot merge elements that are not UpdateRunSnapshots")

        operation = RunOperation()
        operation.MergeFrom(old.operation)
        operation.MergeFrom(new.operation)

        # TODO make sure this is correct
        return AggregatingQueueElement(
            sequence_id=new.sequence_id,
            timestamp=new.timestamp,
            key=new.key,
            operation=operation,
        )

    def stop_blocking(self) -> None:
        self._block = False

    def empty(self) -> bool:
        return self._queue.empty()

    @_cast_to_queue_element
    def get_aggregate(self) -> Optional[AggregatingQueueElement]:
        aggregate: Optional[AggregatingQueueElement] = None
        merged = 0
        current_key: Union[Type[NotSet], Optional[Tuple[int, int]]] = NotSet
        while merged < self._max_batch_size:
            try:
                element = self._get()  # after this line, the placeholder is None
                if current_key is NotSet:
                    current_key = element.key
            except Empty:
                if self._block:
                    continue
                else:
                    break

            if not self._should_aggregate(element.operation):
                current_key = NotSet
                if not aggregate:
                    return element
                else:
                    self._placeholder = element
                    return aggregate
            elif element.key != current_key:
                self._placeholder = element
                current_key = element.key
                return aggregate
            else:
                if aggregate is None:
                    aggregate = element
                    merged = 1
                else:
                    new_aggregate = self._merge_element(aggregate, element)
                    if new_aggregate.operation.ByteSize() > self._batch_max_bytes:
                        self._placeholder = element
                        return aggregate
                    else:
                        aggregate = new_aggregate
                        merged += 1

        return aggregate
