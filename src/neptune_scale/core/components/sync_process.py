from __future__ import annotations

__all__ = ("SyncProcess",)

import multiprocessing
import queue
import signal
import threading
from multiprocessing import (
    Process,
    Queue,
)
from multiprocessing.sharedctypes import Synchronized
from multiprocessing.synchronize import Condition
from types import FrameType
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Literal,
    NamedTuple,
    Optional,
    Type,
    TypeVar,
)

import backoff
import httpx
from neptune_api.errors import (
    InvalidApiTokenException,
    UnableToDeserializeApiKeyError,
    UnableToExchangeApiKeyError,
    UnableToRefreshTokenError,
    UnexpectedStatus,
)
from neptune_api.proto.google_rpc.code_pb2 import Code
from neptune_api.proto.neptune_pb.ingest.v1.ingest_pb2 import IngestCode
from neptune_api.proto.neptune_pb.ingest.v1.pub.client_pb2 import (
    BulkRequestStatus,
    RequestId,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale.api.api_client import (
    ApiClient,
    backend_factory,
)
from neptune_scale.core.components.abstract import (
    Resource,
    WithResources,
)
from neptune_scale.core.components.daemon import Daemon
from neptune_scale.core.components.errors_tracking import ErrorsQueue
from neptune_scale.core.components.queue_element import QueueElement
from neptune_scale.core.logger import logger
from neptune_scale.core.util import safe_signal_name
from neptune_scale.exceptions import (
    NeptuneConnectionLostError,
    NeptuneFieldPathEmpty,
    NeptuneFieldPathExceedsSizeLimit,
    NeptuneFieldPathInvalid,
    NeptuneFieldPathNonWritable,
    NeptuneFieldTypeConflicting,
    NeptuneFieldTypeUnsupported,
    NeptuneFloatValueNanInfUnsupported,
    NeptuneInternalServerError,
    NeptuneInvalidCredentialsError,
    NeptuneOperationsQueueMaxSizeExceeded,
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
    NeptuneUnableToAuthenticateError,
    NeptuneUnauthorizedError,
    NeptuneUnexpectedError,
    NeptuneUnexpectedResponseError,
)
from neptune_scale.parameters import (
    INTERNAL_QUEUE_FEEDER_THREAD_SLEEP_TIME,
    MAX_QUEUE_SIZE,
    MAX_REQUEST_RETRY_SECONDS,
    MAX_REQUESTS_STATUS_BATCH_SIZE,
    SHUTDOWN_TIMEOUT,
    STATUS_TRACKING_THREAD_SLEEP_TIME,
    SYNC_PROCESS_SLEEP_TIME,
    SYNC_THREAD_SLEEP_TIME,
)

T = TypeVar("T")


CODE_TO_ERROR: Dict[IngestCode.ValueType, Optional[Type[Exception]]] = {
    IngestCode.OK: None,
    IngestCode.PROJECT_NOT_FOUND: NeptuneProjectNotFound,
    IngestCode.PROJECT_INVALID_NAME: NeptuneProjectInvalidName,
    IngestCode.RUN_NOT_FOUND: NeptuneRunNotFound,
    IngestCode.RUN_DUPLICATE: NeptuneRunDuplicate,
    IngestCode.RUN_CONFLICTING: NeptuneRunConflicting,
    IngestCode.RUN_FORK_PARENT_NOT_FOUND: NeptuneRunForkParentNotFound,
    IngestCode.RUN_INVALID_CREATION_PARAMETERS: NeptuneRunInvalidCreationParameters,
    IngestCode.FIELD_PATH_EXCEEDS_SIZE_LIMIT: NeptuneFieldPathExceedsSizeLimit,
    IngestCode.FIELD_PATH_EMPTY: NeptuneFieldPathEmpty,
    IngestCode.FIELD_PATH_INVALID: NeptuneFieldPathInvalid,
    IngestCode.FIELD_PATH_NON_WRITABLE: NeptuneFieldPathNonWritable,
    IngestCode.FIELD_TYPE_UNSUPPORTED: NeptuneFieldTypeUnsupported,
    IngestCode.FIELD_TYPE_CONFLICTING: NeptuneFieldTypeConflicting,
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


def code_to_exception(code: IngestCode.ValueType) -> Optional[Type[Exception]]:
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

    def peek(self, max_size: int) -> Optional[List[T]]:
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


def with_api_errors_handling(func: Callable[..., Any]) -> Callable[..., Any]:
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except (InvalidApiTokenException, UnableToDeserializeApiKeyError):
            raise NeptuneInvalidCredentialsError()
        except (UnableToRefreshTokenError, UnableToExchangeApiKeyError, UnexpectedStatus):
            raise NeptuneUnableToAuthenticateError()
        except (httpx.ConnectError, httpx.TimeoutException, httpx.RemoteProtocolError):
            raise NeptuneConnectionLostError()
        except Exception as e:
            raise e

    return wrapper


class SyncProcess(Process):
    def __init__(
        self,
        operations_queue: Queue,
        errors_queue: ErrorsQueue,
        api_token: str,
        project: str,
        family: str,
        mode: Literal["async", "disabled"],
        last_put_seq: Synchronized[int],
        last_put_seq_wait: Condition,
        last_ack_seq: Synchronized[int],
        last_ack_seq_wait: Condition,
        last_ack_timestamp: Synchronized[float],
        last_ack_timestamp_wait: Condition,
        max_queue_size: int = MAX_QUEUE_SIZE,
    ) -> None:
        super().__init__(name="SyncProcess")

        self._external_operations_queue: Queue[QueueElement] = operations_queue
        self._errors_queue: ErrorsQueue = errors_queue
        self._api_token: str = api_token
        self._project: str = project
        self._family: str = family
        self._last_put_seq: Synchronized[int] = last_put_seq
        self._last_put_seq_wait: Condition = last_put_seq_wait
        self._last_ack_seq: Synchronized[int] = last_ack_seq
        self._last_ack_seq_wait: Condition = last_ack_seq_wait
        self._last_ack_timestamp: Synchronized[float] = last_ack_timestamp
        self._last_ack_timestamp_wait: Condition = last_ack_timestamp_wait
        self._max_queue_size: int = max_queue_size
        self._mode: Literal["async", "disabled"] = mode

        # This flag is set when a termination signal is caught
        self._stop_event = multiprocessing.Event()

    def _handle_signal(self, signum: int, frame: Optional[FrameType]) -> None:
        logger.debug(f"Received signal {safe_signal_name(signum)}")
        self._stop_event.set()  # Trigger the stop event

    def run(self) -> None:
        logger.info("Data synchronization started")

        # Register signals handlers
        signal.signal(signal.SIGTERM, self._handle_signal)

        worker = SyncProcessWorker(
            project=self._project,
            family=self._family,
            api_token=self._api_token,
            errors_queue=self._errors_queue,
            external_operations_queue=self._external_operations_queue,
            last_put_seq=self._last_put_seq,
            last_put_seq_wait=self._last_put_seq_wait,
            last_ack_seq=self._last_ack_seq,
            last_ack_seq_wait=self._last_ack_seq_wait,
            max_queue_size=self._max_queue_size,
            last_ack_timestamp=self._last_ack_timestamp,
            last_ack_timestamp_wait=self._last_ack_timestamp_wait,
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


class SyncProcessWorker(WithResources):
    def __init__(
        self,
        *,
        api_token: str,
        project: str,
        family: str,
        mode: Literal["async", "disabled"],
        errors_queue: ErrorsQueue,
        external_operations_queue: multiprocessing.Queue[QueueElement],
        last_put_seq: Synchronized[int],
        last_put_seq_wait: Condition,
        last_ack_seq: Synchronized[int],
        last_ack_seq_wait: Condition,
        last_ack_timestamp: Synchronized[float],
        last_ack_timestamp_wait: Condition,
        max_queue_size: int = MAX_QUEUE_SIZE,
    ) -> None:
        self._errors_queue = errors_queue

        self._internal_operations_queue: queue.Queue[QueueElement] = queue.Queue(maxsize=max_queue_size)
        self._status_tracking_queue: PeekableQueue[StatusTrackingElement] = PeekableQueue()
        self._sync_thread = SenderThread(
            api_token=api_token,
            operations_queue=self._internal_operations_queue,
            status_tracking_queue=self._status_tracking_queue,
            errors_queue=self._errors_queue,
            family=family,
            last_put_seq=last_put_seq,
            last_put_seq_wait=last_put_seq_wait,
            mode=mode,
        )
        self._external_to_internal_thread = InternalQueueFeederThread(
            external=external_operations_queue,
            internal=self._internal_operations_queue,
            errors_queue=self._errors_queue,
        )
        self._status_tracking_thread = StatusTrackingThread(
            api_token=api_token,
            mode=mode,
            project=project,
            errors_queue=self._errors_queue,
            status_tracking_queue=self._status_tracking_queue,
            last_ack_seq=last_ack_seq,
            last_ack_seq_wait=last_ack_seq_wait,
            last_ack_timestamp=last_ack_timestamp,
            last_ack_timestamp_wait=last_ack_timestamp_wait,
        )

    @property
    def threads(self) -> tuple[Daemon, ...]:
        return self._external_to_internal_thread, self._sync_thread, self._status_tracking_thread

    @property
    def resources(self) -> tuple[Resource, ...]:
        return self._external_to_internal_thread, self._sync_thread, self._status_tracking_thread

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


class InternalQueueFeederThread(Daemon, Resource):
    def __init__(
        self,
        external: multiprocessing.Queue[QueueElement],
        internal: queue.Queue[QueueElement],
        errors_queue: ErrorsQueue,
    ) -> None:
        super().__init__(name="InternalQueueFeederThread", sleep_time=INTERNAL_QUEUE_FEEDER_THREAD_SLEEP_TIME)

        self._external: multiprocessing.Queue[QueueElement] = external
        self._internal: queue.Queue[QueueElement] = internal
        self._errors_queue: ErrorsQueue = errors_queue

        self._latest_unprocessed: Optional[QueueElement] = None

    def get_next(self) -> Optional[QueueElement]:
        if self._latest_unprocessed is not None:
            return self._latest_unprocessed

        try:
            self._latest_unprocessed = self._external.get_nowait()
            return self._latest_unprocessed
        except queue.Empty:
            return None

    def commit(self) -> None:
        self._latest_unprocessed = None

    def work(self) -> None:
        try:
            while (operation := self.get_next()) is not None:
                try:
                    logger.debug("Copying operation #%d to internal queue", operation.sequence_id)
                    self._internal.put_nowait(operation)
                    self.commit()
                except queue.Full:
                    logger.debug("Internal queue is full (%d elements), waiting for free space", self._internal.maxsize)
                    self._errors_queue.put(NeptuneOperationsQueueMaxSizeExceeded(max_size=self._internal.maxsize))
                    # Sleep before retry
                    break
        except Exception as e:
            self._errors_queue.put(e)
            self.interrupt()
            raise NeptuneSynchronizationStopped() from e


class SenderThread(Daemon, WithResources):
    def __init__(
        self,
        api_token: str,
        family: str,
        operations_queue: queue.Queue[QueueElement],
        status_tracking_queue: PeekableQueue[StatusTrackingElement],
        errors_queue: ErrorsQueue,
        last_put_seq: Synchronized[int],
        last_put_seq_wait: Condition,
        mode: Literal["async", "disabled"],
    ) -> None:
        super().__init__(name="SenderThread", sleep_time=SYNC_THREAD_SLEEP_TIME)

        self._api_token: str = api_token
        self._family: str = family
        self._operations_queue: queue.Queue[QueueElement] = operations_queue
        self._status_tracking_queue: PeekableQueue[StatusTrackingElement] = status_tracking_queue
        self._errors_queue: ErrorsQueue = errors_queue
        self._last_put_seq: Synchronized[int] = last_put_seq
        self._last_put_seq_wait: Condition = last_put_seq_wait
        self._mode: Literal["async", "disabled"] = mode

        self._backend: Optional[ApiClient] = None
        self._latest_unprocessed: Optional[QueueElement] = None

    def get_next(self) -> Optional[QueueElement]:
        if self._latest_unprocessed is not None:
            return self._latest_unprocessed

        try:
            self._latest_unprocessed = self._operations_queue.get_nowait()
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

    @backoff.on_exception(backoff.expo, NeptuneConnectionLostError, max_time=MAX_REQUEST_RETRY_SECONDS)
    @with_api_errors_handling
    def submit(self, *, operation: RunOperation) -> Optional[RequestId]:
        if self._backend is None:
            self._backend = backend_factory(api_token=self._api_token, mode=self._mode)

        response = self._backend.submit(operation=operation, family=self._family)

        if response.status_code == 403:
            raise NeptuneUnauthorizedError()

        if response.status_code != 200:
            logger.error("HTTP response error: %s", response.status_code)
            if response.status_code // 100 == 5:
                raise NeptuneInternalServerError()
            else:
                raise NeptuneUnexpectedResponseError()

        return response.parsed

    def work(self) -> None:
        try:
            while (operation := self.get_next()) is not None:
                sequence_id, timestamp, data = operation

                try:
                    logger.debug("Submitting operation #%d with size of %d bytes", sequence_id, len(data))
                    run_operation = RunOperation()
                    run_operation.ParseFromString(data)
                    request_id = self.submit(operation=run_operation)

                    if request_id is None:
                        raise NeptuneUnexpectedError("Server response is empty")

                    logger.debug("Operation #%d submitted as %s", sequence_id, request_id.value)
                    self._status_tracking_queue.put(
                        StatusTrackingElement(sequence_id=sequence_id, request_id=request_id.value, timestamp=timestamp)
                    )
                    self.commit()
                except NeptuneRetryableError as e:
                    self._errors_queue.put(e)
                    # Sleep before retry
                    break

                # Update Last PUT sequence id and notify threads in the main process
                with self._last_put_seq_wait:
                    self._last_put_seq.value = sequence_id
                    self._last_put_seq_wait.notify_all()
        except Exception as e:
            self._errors_queue.put(e)
            self._last_put_seq_wait.notify_all()
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
        last_ack_seq: Synchronized[int],
        last_ack_seq_wait: Condition,
        last_ack_timestamp: Synchronized[float],
        last_ack_timestamp_wait: Condition,
    ) -> None:
        super().__init__(name="StatusTrackingThread", sleep_time=STATUS_TRACKING_THREAD_SLEEP_TIME)

        self._api_token: str = api_token
        self._mode: Literal["async", "disabled"] = mode
        self._project: str = project
        self._errors_queue: ErrorsQueue = errors_queue
        self._status_tracking_queue: PeekableQueue[StatusTrackingElement] = status_tracking_queue
        self._last_ack_seq: Synchronized[int] = last_ack_seq
        self._last_ack_seq_wait: Condition = last_ack_seq_wait
        self._last_ack_timestamp: Synchronized[float] = last_ack_timestamp
        self._last_ack_timestamp_wait: Condition = last_ack_timestamp_wait

        self._backend: Optional[ApiClient] = None

    @property
    def resources(self) -> tuple[Resource, ...]:
        if self._backend is not None:
            return (self._backend,)
        return ()

    def get_next(self) -> Optional[List[StatusTrackingElement]]:
        try:
            return self._status_tracking_queue.peek(max_size=MAX_REQUESTS_STATUS_BATCH_SIZE)
        except queue.Empty:
            return None

    @backoff.on_exception(backoff.expo, NeptuneConnectionLostError, max_time=MAX_REQUEST_RETRY_SECONDS)
    @with_api_errors_handling
    def check_batch(self, *, request_ids: List[str]) -> Optional[BulkRequestStatus]:
        if self._backend is None:
            self._backend = backend_factory(api_token=self._api_token, mode=self._mode)

        response = self._backend.check_batch(request_ids=request_ids, project=self._project)

        if response.status_code == 403:
            raise NeptuneUnauthorizedError()

        if response.status_code != 200:
            logger.error("HTTP response error: %s", response.status_code)
            if response.status_code // 100 == 5:
                raise NeptuneInternalServerError()
            else:
                raise NeptuneUnexpectedResponseError()

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

                        with self._last_ack_seq_wait:
                            self._last_ack_seq.value = processed_sequence_id
                            self._last_ack_seq_wait.notify_all()

                    # Update Last ACK timestamp and notify threads in the main process
                    if processed_timestamp is not None:
                        with self._last_ack_timestamp_wait:
                            self._last_ack_timestamp.value = processed_timestamp
                            self._last_ack_timestamp_wait.notify_all()
                else:
                    # Sleep before retry
                    break
        except Exception as e:
            self._errors_queue.put(e)
            self.interrupt()
            self._last_ack_seq_wait.notify_all()
            raise NeptuneSynchronizationStopped() from e
