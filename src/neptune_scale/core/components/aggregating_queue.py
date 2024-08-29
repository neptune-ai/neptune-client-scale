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
from neptune_scale.core.logger import logger
from neptune_scale.core.validation import verify_type
from neptune_scale.parameters import AGGREGATING_QUEUE_BLOCKING_TIMEOUT


class NotSet:
    pass


@dataclass
class AggregatingQueueElement:
    first_sequence_id: int
    last_sequence_id: int
    first_timestamp: float
    last_timestamp: float
    key: Optional[Tuple[int, int]]
    operation: RunOperation


def _cast_to_queue_element(
    fn: Callable[..., Optional[AggregatingQueueElement]],
) -> Callable[..., Optional[QueueElement]]:
    def wrapper(*args: Any, **kwargs: Any) -> Optional[QueueElement]:
        result = fn(*args, **kwargs)
        if result is None:
            return None
        logger.debug(
            f"Returning AggregatingQueueElement. Aggreagated sequence ids from {result.first_sequence_id} to {result.last_sequence_id}. "
            f"Aggregated timestamps from {result.first_timestamp} to {result.last_timestamp}"
        )
        return QueueElement(
            sequence_id=result.last_sequence_id,
            timestamp=result.last_timestamp,
            operation=result.operation.SerializeToString(),
        )

    return wrapper


class AggregatingQueue:
    def __init__(
        self,
        max_batch_size: int,
        queue_size: Optional[int] = None,
        block: bool = True,
        batch_max_bytes: Optional[int] = None,
        timeout: float = AGGREGATING_QUEUE_BLOCKING_TIMEOUT,
    ) -> None:
        verify_type("max_batch_size", max_batch_size, int)
        verify_type("queue_size", queue_size, (int, type(None)))
        verify_type("block", block, bool)
        verify_type("batch_max_bytes", batch_max_bytes, (int, type(None)))
        verify_type("timeout", timeout, float)

        self._queue_size = queue_size
        self._queue: Queue[AggregatingQueueElement] = Queue(
            maxsize=queue_size if queue_size is not None else -1
        )  # unbounded size
        self._max_batch_size = max_batch_size
        self._last_unprocessed: Optional[AggregatingQueueElement] = None
        self._block = block
        self._timeout = timeout
        self._batch_max_bytes = batch_max_bytes if batch_max_bytes is not None else float("inf")

    def put(self, element: QueueElement) -> None:
        # TODO: consider adding `step` to QueueElement to avoid deserializing protobufs just
        #       to compare steps
        operation = RunOperation()
        operation.ParseFromString(element.operation)
        key = self._make_key(operation)

        self._queue.put(
            AggregatingQueueElement(
                first_sequence_id=element.sequence_id,
                last_sequence_id=element.sequence_id,
                first_timestamp=element.timestamp,
                last_timestamp=element.timestamp,
                key=key,
                operation=operation,
            )
        )

    def _get(self) -> AggregatingQueueElement:
        if self._last_unprocessed is not None:
            placeholder = self._last_unprocessed
            self._last_unprocessed = None
            return placeholder

        return self._queue.get(timeout=self._timeout)

    def _should_aggregate(self, op: RunOperation) -> bool:
        return op.HasField("update")

    def _make_key(self, value: RunOperation) -> Tuple[int, int]:
        if self._should_aggregate(value):
            step = value.update.step
            if step is not None:
                return 0, hash((step.whole, step.micro))
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
            first_sequence_id=old.first_sequence_id,
            last_sequence_id=new.last_sequence_id,
            first_timestamp=old.first_timestamp,
            last_timestamp=new.last_timestamp,
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
                element = self._get()  # after this line, self._last_unprocessed is None
                if current_key is NotSet:
                    current_key = element.key
            except Empty:
                if self._block:
                    continue
                else:
                    break

            if not self._should_aggregate(element.operation):
                if not aggregate:
                    return element
                else:
                    self._last_unprocessed = element
                    return aggregate
            elif element.key != current_key:
                self._last_unprocessed = element
                return aggregate
            else:
                if aggregate is None:
                    aggregate = element
                    merged = 1
                else:
                    new_aggregate = self._merge_element(aggregate, element)
                    if new_aggregate.operation.ByteSize() > self._batch_max_bytes:
                        self._last_unprocessed = element
                        return aggregate
                    else:
                        aggregate = new_aggregate
                        merged += 1

        return aggregate
