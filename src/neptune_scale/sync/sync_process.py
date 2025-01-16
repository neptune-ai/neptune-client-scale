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

__all__ = ("SyncProcess",)

import multiprocessing
import queue
import signal
import threading
from collections.abc import Iterable
from multiprocessing import (
    Process,
    Queue,
)
from types import FrameType
from typing import (
    Generic,
    Literal,
    NamedTuple,
    Optional,
    Protocol,
    TypeVar,
)

import backoff
from neptune_api.proto.google_rpc.code_pb2 import Code
from neptune_api.proto.neptune_pb.ingest.v1.ingest_pb2 import IngestCode
from neptune_api.proto.neptune_pb.ingest.v1.pub.client_pb2 import (
    BulkRequestStatus,
    SubmitResponse,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale.exceptions import (
    NeptuneAttributePathEmpty,
    NeptuneAttributePathExceedsSizeLimit,
    NeptuneAttributePathInvalid,
    NeptuneAttributePathNonWritable,
    NeptuneAttributeTypeMismatch,
    NeptuneAttributeTypeUnsupported,
    NeptuneFloatValueNanInfUnsupported,
    NeptuneProjectInvalidName,
    NeptuneProjectNotFound,
    NeptuneRetryableError,
    NeptuneRunConflicting,
    NeptuneRunDuplicate,
    NeptuneRunForkParentNotFound,
    NeptuneRunInvalidCreationParameters,
    NeptuneRunNotFound,
    NeptuneSeriesPointDuplicate,
    NeptuneSeriesStepNonIncreasing,
    NeptuneSeriesStepNotAfterForkPoint,
    NeptuneSeriesTimestampDecreasing,
    NeptuneStringSetExceedsSizeLimit,
    NeptuneStringValueExceedsSizeLimit,
    NeptuneSynchronizationStopped,
    NeptuneUnexpectedError,
)
from neptune_scale.net.api_client import (
    ApiClient,
    backend_factory,
    with_api_errors_handling,
)
from neptune_scale.net.util import raise_for_http_status
from neptune_scale.sync.aggregating_queue import AggregatingQueue
from neptune_scale.sync.errors_tracking import ErrorsQueue
from neptune_scale.sync.parameters import (
    INTERNAL_QUEUE_FEEDER_THREAD_SLEEP_TIME,
    MAX_QUEUE_SIZE,
    MAX_REQUEST_RETRY_SECONDS,
    MAX_REQUESTS_STATUS_BATCH_SIZE,
    SHUTDOWN_TIMEOUT,
    STATUS_TRACKING_THREAD_SLEEP_TIME,
    SYNC_PROCESS_SLEEP_TIME,
    SYNC_THREAD_SLEEP_TIME,
)
from neptune_scale.sync.queue_element import (
    BatchedOperations,
    SingleOperation,
)
from neptune_scale.sync.util import safe_signal_name
from neptune_scale.util import (
    Daemon,
    ProcessLink,
    SharedFloat,
    SharedInt,
    get_logger,
)
from neptune_scale.util.abstract import (
    Resource,
    WithResources,
)

T = TypeVar("T")

logger = get_logger()

CODE_TO_ERROR: dict[IngestCode.ValueType, Optional[type[Exception]]] = {
    IngestCode.OK: None,
    IngestCode.PROJECT_NOT_FOUND: NeptuneProjectNotFound,
    IngestCode.PROJECT_INVALID_NAME: NeptuneProjectInvalidName,
    IngestCode.RUN_NOT_FOUND: NeptuneRunNotFound,
    IngestCode.RUN_DUPLICATE: NeptuneRunDuplicate,
    IngestCode.RUN_CONFLICTING: NeptuneRunConflicting,
    IngestCode.RUN_FORK_PARENT_NOT_FOUND: NeptuneRunForkParentNotFound,
    IngestCode.RUN_INVALID_CREATION_PARAMETERS: NeptuneRunInvalidCreationParameters,
    IngestCode.FIELD_PATH_EXCEEDS_SIZE_LIMIT: NeptuneAttributePathExceedsSizeLimit,
    IngestCode.FIELD_PATH_EMPTY: NeptuneAttributePathEmpty,
    IngestCode.FIELD_PATH_INVALID: NeptuneAttributePathInvalid,
    IngestCode.FIELD_PATH_NON_WRITABLE: NeptuneAttributePathNonWritable,
    IngestCode.FIELD_TYPE_UNSUPPORTED: NeptuneAttributeTypeUnsupported,
    IngestCode.FIELD_TYPE_CONFLICTING: NeptuneAttributeTypeMismatch,
    IngestCode.SERIES_POINT_DUPLICATE: NeptuneSeriesPointDuplicate,
    IngestCode.SERIES_STEP_NON_INCREASING: NeptuneSeriesStepNonIncreasing,
    IngestCode.SERIES_STEP_NOT_AFTER_FORK_POINT: NeptuneSeriesStepNotAfterForkPoint,
    IngestCode.SERIES_TIMESTAMP_DECREASING: NeptuneSeriesTimestampDecreasing,
    IngestCode.FLOAT_VALUE_NAN_INF_UNSUPPORTED: NeptuneFloatValueNanInfUnsupported,
    IngestCode.STRING_VALUE_EXCEEDS_SIZE_LIMIT: NeptuneStringValueExceedsSizeLimit,
    IngestCode.STRING_SET_EXCEEDS_SIZE_LIMIT: NeptuneStringSetExceedsSizeLimit,
}


class StatusTrackingElement(NamedTuple):
    sequence_id: int
    timestamp: float
    request_id: str


def code_to_exception(code: IngestCode.ValueType) -> Optional[type[Exception]]:
    if code in CODE_TO_ERROR:
        return CODE_TO_ERROR[code]
    return NeptuneUnexpectedError


class PeekableQueue(Generic[T]):
    def __init__(self) -> None:
        self._lock: threading.RLock = threading.RLock()
        self._queue: queue.Queue[T] = queue.Queue()

    def put(self, element: T) -> None:
        with self._lock:
            self._queue.put(element)

    def peek(self, max_size: int) -> Optional[list[T]]:
        with self._lock:
            size = self._queue.qsize()
            if size == 0:
                return None

            items = []
            for i in range(min(size, max_size)):
                item = self._queue.queue[i]
                items.append(item)
            return items

    def commit(self, n: int) -> None:
        with self._lock:
            size = self._queue.qsize()
            for _ in range(min(size, n)):
                self._queue.get()


class SyncProcess(Process):
    def __init__(
        self,
        operations_queue: Queue,
        errors_queue: ErrorsQueue,
        process_link: ProcessLink,
        api_token: str,
        project: str,
        family: str,
        mode: Literal["async", "disabled"],
        last_queued_seq: SharedInt,
        last_ack_seq: SharedInt,
        last_ack_timestamp: SharedFloat,
        max_queue_size: int = MAX_QUEUE_SIZE,
    ) -> None:
        super().__init__(name="SyncProcess")

        self._input_operations_queue: Queue[SingleOperation] = operations_queue
        self._errors_queue: ErrorsQueue = errors_queue
        self._process_link: ProcessLink = process_link
        self._api_token: str = api_token
        self._project: str = project
        self._family: str = family
        self._last_queued_seq: SharedInt = last_queued_seq
        self._last_ack_seq: SharedInt = last_ack_seq
        self._last_ack_timestamp: SharedFloat = last_ack_timestamp
        self._max_queue_size: int = max_queue_size
        self._mode: Literal["async", "disabled"] = mode

        # This flag is set when a termination signal is caught
        self._stop_event = multiprocessing.Event()

    def _handle_signal(self, signum: int, frame: Optional[FrameType]) -> None:
        logger.debug(f"Received signal {safe_signal_name(signum)}")
        self._stop_event.set()  # Trigger the stop event

    def _on_parent_link_closed(self, _: ProcessLink) -> None:
        logger.error("SyncProcess: main process closed unexpectedly. Exiting")
        self._stop_event.set()

    def run(self) -> None:
        logger.info("Data synchronization started")

        self._process_link.start(on_link_closed=self._on_parent_link_closed)
        signal.signal(signal.SIGTERM, self._handle_signal)

        worker = SyncProcessWorker(
            project=self._project,
            family=self._family,
            api_token=self._api_token,
            errors_queue=self._errors_queue,
            input_queue=self._input_operations_queue,
            last_queued_seq=self._last_queued_seq,
            last_ack_seq=self._last_ack_seq,
            max_queue_size=self._max_queue_size,
            last_ack_timestamp=self._last_ack_timestamp,
            mode=self._mode,
        )
        worker.start()
        try:
            while not self._stop_event.is_set():
                worker.join(timeout=SYNC_PROCESS_SLEEP_TIME)
        except KeyboardInterrupt:
            logger.debug("Data synchronization interrupted by user")
        finally:
            logger.info("Data synchronization stopping")
            worker.interrupt()
            worker.wake_up()
            worker.join(timeout=SHUTDOWN_TIMEOUT)
            worker.close()
        logger.info("Data synchronization finished")


class SupportsPutNowait(Protocol):
    def put_nowait(self, element: SingleOperation) -> None: ...


class SyncProcessWorker(WithResources):
    def __init__(
        self,
        *,
        api_token: str,
        project: str,
        family: str,
        mode: Literal["async", "disabled"],
        errors_queue: ErrorsQueue,
        input_queue: multiprocessing.Queue[SingleOperation],
        last_queued_seq: SharedInt,
        last_ack_seq: SharedInt,
        last_ack_timestamp: SharedFloat,
        max_queue_size: int = MAX_QUEUE_SIZE,
    ) -> None:
        self._errors_queue = errors_queue

        self._internal_operations_queue: AggregatingQueue = AggregatingQueue(max_queue_size=max_queue_size)
        self._status_tracking_queue: PeekableQueue[StatusTrackingElement] = PeekableQueue()
        self._sync_thread = SenderThread(
            api_token=api_token,
            operations_queue=self._internal_operations_queue,
            status_tracking_queue=self._status_tracking_queue,
            errors_queue=self._errors_queue,
            family=family,
            last_queued_seq=last_queued_seq,
            mode=mode,
        )

        self._status_tracking_thread = StatusTrackingThread(
            api_token=api_token,
            mode=mode,
            project=project,
            errors_queue=self._errors_queue,
            status_tracking_queue=self._status_tracking_queue,
            last_ack_seq=last_ack_seq,
            last_ack_timestamp=last_ack_timestamp,
        )

        self._operation_dispatcher_thread = OperationDispatcherThread(
            input_queue=input_queue,
            consumers=[self._internal_operations_queue],
            errors_queue=self._errors_queue,
        )

    @property
    def threads(self) -> tuple[Daemon, ...]:
        return self._operation_dispatcher_thread, self._sync_thread, self._status_tracking_thread

    @property
    def resources(self) -> tuple[Resource, ...]:
        return self._operation_dispatcher_thread, self._sync_thread, self._status_tracking_thread

    def interrupt(self) -> None:
        for thread in self.threads:
            thread.interrupt()

    def wake_up(self) -> None:
        for thread in self.threads:
            thread.wake_up()

    def start(self) -> None:
        for thread in self.threads:
            thread.start()

    def join(self, timeout: Optional[int] = None) -> None:
        # The same timeout will be applied to each thread separately
        for thread in self.threads:
            thread.join(timeout=timeout)


class OperationDispatcherThread(Daemon, Resource):
    """Reads incoming messages from a multiprocessing.Queue, and dispatches them to a list of consumers,
    which can be of type `queue.Queue`, but also any other object that supports put_nowait() method.

    If any of the consumers' put_nowait() raises queue.Full, the thread will stop processing further operations.
    """

    def __init__(
        self,
        input_queue: multiprocessing.Queue[SingleOperation],
        consumers: Iterable[SupportsPutNowait],
        errors_queue: ErrorsQueue,
    ) -> None:
        super().__init__(name="OperationDispatcherThread", sleep_time=INTERNAL_QUEUE_FEEDER_THREAD_SLEEP_TIME)

        self._input_queue: multiprocessing.Queue[SingleOperation] = input_queue
        self._consumers = tuple(consumers)
        self._errors_queue: ErrorsQueue = errors_queue

        self._latest_unprocessed: Optional[SingleOperation] = None

    def get_next(self) -> Optional[SingleOperation]:
        if self._latest_unprocessed is not None:
            return self._latest_unprocessed

        try:
            self._latest_unprocessed = self._input_queue.get(timeout=INTERNAL_QUEUE_FEEDER_THREAD_SLEEP_TIME)
            return self._latest_unprocessed
        except queue.Empty:
            return None

    def commit(self) -> None:
        self._latest_unprocessed = None

    def work(self) -> None:
        try:
            while not self._is_interrupted():
                if (operation := self.get_next()) is None:
                    continue

                try:
                    for consumer in self._consumers:
                        consumer.put_nowait(operation)
                    self.commit()
                except queue.Full as e:
                    # We have two ways to deal with this situation:
                    # 1. Consider this a fatal error, and stop processing further operations.
                    # 2. Retry, assuming that any consumer that _did_ manage to receive the operation, is
                    #    idempotent and can handle the same operation again.
                    #
                    # Currently, we choose 1.
                    logger.error("Operation queue overflow. Neptune will not process further operations.")
                    raise e
        except Exception as e:
            self._errors_queue.put(e)
            self.interrupt()
            raise NeptuneSynchronizationStopped() from e


class SenderThread(Daemon, WithResources):
    def __init__(
        self,
        api_token: str,
        family: str,
        operations_queue: AggregatingQueue,
        status_tracking_queue: PeekableQueue[StatusTrackingElement],
        errors_queue: ErrorsQueue,
        last_queued_seq: SharedInt,
        mode: Literal["async", "disabled"],
    ) -> None:
        super().__init__(name="SenderThread", sleep_time=SYNC_THREAD_SLEEP_TIME)

        self._api_token: str = api_token
        self._family: str = family
        self._operations_queue: AggregatingQueue = operations_queue
        self._status_tracking_queue: PeekableQueue[StatusTrackingElement] = status_tracking_queue
        self._errors_queue: ErrorsQueue = errors_queue
        self._last_queued_seq: SharedInt = last_queued_seq
        self._mode: Literal["async", "disabled"] = mode

        self._backend: Optional[ApiClient] = None
        self._latest_unprocessed: Optional[BatchedOperations] = None

    def get_next(self) -> Optional[BatchedOperations]:
        if self._latest_unprocessed is not None:
            return self._latest_unprocessed

        try:
            self._latest_unprocessed = self._operations_queue.get()
            return self._latest_unprocessed
        except queue.Empty:
            return None

    def commit(self) -> None:
        self._latest_unprocessed = None

    @property
    def resources(self) -> tuple[Resource, ...]:
        if self._backend is not None:
            return (self._backend,)
        return ()

    @backoff.on_exception(backoff.expo, NeptuneRetryableError, max_time=MAX_REQUEST_RETRY_SECONDS)
    @with_api_errors_handling
    def submit(self, *, operation: RunOperation) -> Optional[SubmitResponse]:
        if self._backend is None:
            self._backend = backend_factory(api_token=self._api_token, mode=self._mode)

        response = self._backend.submit(operation=operation, family=self._family)

        status_code = response.status_code
        if status_code != 200:
            raise_for_http_status(status_code)

        return response.parsed

    def work(self) -> None:
        try:
            # TODO: is there a point in serializing the data on AggregatingQueue? It does not move between processes,
            #       so we could just pass around instances of RunOperation
            while (operation := self.get_next()) is not None:
                sequence_id, timestamp, data = operation

                try:
                    logger.debug("Submitting operation #%d with size of %d bytes", sequence_id, len(data))
                    run_operation = RunOperation()
                    run_operation.ParseFromString(data)
                    request_ids: Optional[SubmitResponse] = self.submit(operation=run_operation)

                    if request_ids is None or not request_ids.request_ids:
                        raise NeptuneUnexpectedError("Server response is empty")

                    last_request_id = request_ids.request_ids[-1]

                    logger.debug("Operation #%d submitted as %s", sequence_id, last_request_id)
                    self._status_tracking_queue.put(
                        StatusTrackingElement(sequence_id=sequence_id, request_id=last_request_id, timestamp=timestamp)
                    )
                    self.commit()
                except NeptuneRetryableError as e:
                    self._errors_queue.put(e)
                    # Sleep before retry
                    break

                # Update Last PUT sequence id and notify threads in the main process
                with self._last_queued_seq:
                    self._last_queued_seq.value = sequence_id
                    self._last_queued_seq.notify_all()
        except Exception as e:
            self._errors_queue.put(e)
            with self._last_queued_seq:
                self._last_queued_seq.notify_all()
            self.interrupt()
            raise NeptuneSynchronizationStopped() from e


class StatusTrackingThread(Daemon, WithResources):
    def __init__(
        self,
        api_token: str,
        mode: Literal["async", "disabled"],
        project: str,
        errors_queue: ErrorsQueue,
        status_tracking_queue: PeekableQueue[StatusTrackingElement],
        last_ack_seq: SharedInt,
        last_ack_timestamp: SharedFloat,
    ) -> None:
        super().__init__(name="StatusTrackingThread", sleep_time=STATUS_TRACKING_THREAD_SLEEP_TIME)

        self._api_token: str = api_token
        self._mode: Literal["async", "disabled"] = mode
        self._project: str = project
        self._errors_queue: ErrorsQueue = errors_queue
        self._status_tracking_queue: PeekableQueue[StatusTrackingElement] = status_tracking_queue
        self._last_ack_seq: SharedInt = last_ack_seq
        self._last_ack_timestamp: SharedFloat = last_ack_timestamp

        self._backend: Optional[ApiClient] = None

    @property
    def resources(self) -> tuple[Resource, ...]:
        if self._backend is not None:
            return (self._backend,)
        return ()

    def get_next(self) -> Optional[list[StatusTrackingElement]]:
        try:
            return self._status_tracking_queue.peek(max_size=MAX_REQUESTS_STATUS_BATCH_SIZE)
        except queue.Empty:
            return None

    @backoff.on_exception(backoff.expo, NeptuneRetryableError, max_time=MAX_REQUEST_RETRY_SECONDS)
    @with_api_errors_handling
    def check_batch(self, *, request_ids: list[str]) -> Optional[BulkRequestStatus]:
        if self._backend is None:
            self._backend = backend_factory(api_token=self._api_token, mode=self._mode)

        response = self._backend.check_batch(request_ids=request_ids, project=self._project)

        status_code = response.status_code

        if status_code != 200:
            raise_for_http_status(status_code)

        return response.parsed

    def work(self) -> None:
        try:
            while (batch := self.get_next()) is not None:
                request_ids = [element.request_id for element in batch]
                sequence_ids = [element.sequence_id for element in batch]
                timestamps = [element.timestamp for element in batch]

                try:
                    response = self.check_batch(request_ids=request_ids)
                    if response is None:
                        raise NeptuneUnexpectedError("Server response is empty")
                except NeptuneRetryableError as e:
                    self._errors_queue.put(e)
                    # Small give up, sleep before retry
                    break

                operations_to_commit, processed_sequence_id, processed_timestamp = 0, None, None
                for request_status, request_sequence_id, timestamp in zip(response.statuses, sequence_ids, timestamps):
                    if any(code_status.code == Code.UNAVAILABLE for code_status in request_status.code_by_count):
                        logger.debug(f"Operation #{request_sequence_id} is not yet processed.")
                        # Request status not ready yet, sleep and retry
                        break

                    for code_status in request_status.code_by_count:
                        if code_status.code != Code.OK and (error := code_to_exception(code_status.detail)) is not None:
                            self._errors_queue.put(error())

                    operations_to_commit += 1
                    processed_sequence_id, processed_timestamp = request_sequence_id, timestamp

                if operations_to_commit > 0:
                    self._status_tracking_queue.commit(operations_to_commit)

                    # Update Last ACK sequence id and notify threads in the main process
                    if processed_sequence_id is not None:
                        logger.debug(f"Operations up to #{processed_sequence_id} are completed.")

                        with self._last_ack_seq:
                            self._last_ack_seq.value = processed_sequence_id
                            self._last_ack_seq.notify_all()

                    # Update Last ACK timestamp and notify threads in the main process
                    if processed_timestamp is not None:
                        with self._last_ack_timestamp:
                            self._last_ack_timestamp.value = processed_timestamp
                            self._last_ack_timestamp.notify_all()
                else:
                    # Sleep before retry
                    break
        except Exception as e:
            self._errors_queue.put(e)
            self.interrupt()
            self._last_ack_seq.notify_all()
            raise NeptuneSynchronizationStopped() from e
