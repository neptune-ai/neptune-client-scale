from __future__ import annotations

from pathlib import Path

from neptune_scale.sync.operations_repository import (
    Metadata,
    Operation,
    OperationsRepository,
    OperationType,
    SequenceId,
)

__all__ = ("SyncProcess",)
import datetime
import multiprocessing
import queue
import signal
import threading
from collections.abc import Generator
from multiprocessing import Process
from types import FrameType
from typing import (
    Generic,
    Literal,
    NamedTuple,
    Optional,
    TypeVar,
)

import backoff
from neptune_api.proto.google_rpc.code_pb2 import Code
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import UpdateRunSnapshots
from neptune_api.proto.neptune_pb.ingest.v1.ingest_pb2 import IngestCode
from neptune_api.proto.neptune_pb.ingest.v1.pub.client_pb2 import (
    BulkRequestStatus,
    SubmitResponse,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale.exceptions import (
    GenericFloatValueNanInfUnsupported,
    NeptuneAttributePathEmpty,
    NeptuneAttributePathExceedsSizeLimit,
    NeptuneAttributePathInvalid,
    NeptuneAttributePathNonWritable,
    NeptuneAttributeTypeMismatch,
    NeptuneAttributeTypeUnsupported,
    NeptuneConnectionLostError,
    NeptuneInternalServerError,
    NeptunePreviewStepNotAfterLastCommittedStep,
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
    NeptuneTooManyRequestsResponseError,
    NeptuneUnauthorizedError,
    NeptuneUnexpectedError,
    NeptuneUnexpectedResponseError,
)
from neptune_scale.net.api_client import (
    ApiClient,
    backend_factory,
    with_api_errors_handling,
)
from neptune_scale.sync.errors_tracking import ErrorsQueue
from neptune_scale.sync.parameters import (
    MAX_REQUEST_RETRY_SECONDS,
    MAX_REQUESTS_STATUS_BATCH_SIZE,
    SHUTDOWN_TIMEOUT,
    STATUS_TRACKING_THREAD_SLEEP_TIME,
    SYNC_PROCESS_SLEEP_TIME,
    SYNC_THREAD_SLEEP_TIME,
)
from neptune_scale.sync.util import safe_signal_name
from neptune_scale.util import (
    Daemon,
    ProcessLink,
    SharedFloat,
    SharedInt,
    get_logger,
)

T = TypeVar("T")

logger = get_logger()

CODE_TO_ERROR: dict[IngestCode.ValueType, Optional[type[Exception]]] = {
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
    IngestCode.FLOAT_VALUE_NAN_INF_UNSUPPORTED: GenericFloatValueNanInfUnsupported,
    IngestCode.STRING_VALUE_EXCEEDS_SIZE_LIMIT: NeptuneStringValueExceedsSizeLimit,
    IngestCode.STRING_SET_EXCEEDS_SIZE_LIMIT: NeptuneStringSetExceedsSizeLimit,
    IngestCode.SERIES_PREVIEW_STEP_NOT_AFTER_LAST_COMMITTED_STEP: NeptunePreviewStepNotAfterLastCommittedStep,
}


class StatusTrackingElement(NamedTuple):
    sequence_id: SequenceId
    timestamp: datetime.datetime
    request_id: str


def code_to_exception(code: IngestCode.ValueType) -> Exception:
    if code in CODE_TO_ERROR:
        error = CODE_TO_ERROR[code]
        return error()  # type: ignore
    return NeptuneUnexpectedError(reason=f"Unexpected ingestion error code: {code}")


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
        operations_repository_path: Path,
        errors_queue: ErrorsQueue,
        process_link: ProcessLink,
        api_token: str,
        project: str,
        family: str,
        mode: Literal["async", "disabled"],
        last_queued_seq: SharedInt,
        last_ack_seq: SharedInt,
        last_ack_timestamp: SharedFloat,
    ) -> None:
        super().__init__(name="SyncProcess")

        self.operations_repository_path: Path = operations_repository_path
        self._errors_queue: ErrorsQueue = errors_queue
        self._process_link: ProcessLink = process_link
        self._api_token: str = api_token
        self._project: str = project
        self._family: str = family
        self._last_queued_seq: SharedInt = last_queued_seq
        self._last_ack_seq: SharedInt = last_ack_seq
        self._last_ack_timestamp: SharedFloat = last_ack_timestamp
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

        status_tracking_queue: PeekableQueue[StatusTrackingElement] = PeekableQueue()
        operations_repository = OperationsRepository(db_path=self.operations_repository_path)
        threads = [
            SenderThread(
                api_token=self._api_token,
                operations_repository=operations_repository,
                status_tracking_queue=status_tracking_queue,
                errors_queue=self._errors_queue,
                family=self._family,
                last_queued_seq=self._last_queued_seq,
                mode=self._mode,
            ),
            StatusTrackingThread(
                api_token=self._api_token,
                mode=self._mode,
                project=self._project,
                errors_queue=self._errors_queue,
                status_tracking_queue=status_tracking_queue,
                last_ack_seq=self._last_ack_seq,
                last_ack_timestamp=self._last_ack_timestamp,
            ),
        ]
        for thread in threads:
            thread.start()

        try:
            while not self._stop_event.is_set():
                for thread in threads:
                    thread.join(timeout=SYNC_PROCESS_SLEEP_TIME)
        except KeyboardInterrupt:
            logger.debug("Data synchronization interrupted by user")
        finally:
            logger.info("Data synchronization stopping")
            for thread in threads:
                thread.interrupt()
                thread.wake_up()

            for thread in threads:
                thread.join(timeout=SHUTDOWN_TIMEOUT)
                thread.close()  # type: ignore
            operations_repository.close()
        logger.info("Data synchronization finished")


class SenderThread(Daemon):
    def __init__(
        self,
        api_token: str,
        family: str,
        operations_repository: OperationsRepository,
        status_tracking_queue: PeekableQueue[StatusTrackingElement],
        errors_queue: ErrorsQueue,
        last_queued_seq: SharedInt,
        mode: Literal["async", "disabled"],
    ) -> None:
        super().__init__(name="SenderThread", sleep_time=SYNC_THREAD_SLEEP_TIME)

        self._api_token: str = api_token
        self._family: str = family
        self._operations_repository: OperationsRepository = operations_repository
        self._status_tracking_queue: PeekableQueue[StatusTrackingElement] = status_tracking_queue
        self._errors_queue: ErrorsQueue = errors_queue
        self._last_queued_seq: SharedInt = last_queued_seq
        self._mode: Literal["async", "disabled"] = mode

        self._backend: Optional[ApiClient] = None
        self._metadata: Metadata = operations_repository.get_metadata()  # type: ignore

    @backoff.on_exception(backoff.expo, NeptuneRetryableError, max_time=MAX_REQUEST_RETRY_SECONDS)
    @with_api_errors_handling
    def submit(self, *, operation: RunOperation) -> Optional[SubmitResponse]:
        if self._backend is None:
            self._backend = backend_factory(api_token=self._api_token, mode=self._mode)

        response = self._backend.submit(operation=operation, family=self._family)

        status_code = response.status_code
        if status_code != 200:
            _raise_exception(status_code)

        return response.parsed

    def work(self) -> None:
        try:
            generator = _stream_operations(self._operations_repository, self._metadata.run_id, self._metadata.project)
            while (operation := next(generator, None)) is not None:
                run_operation, sequence_id, timestamp = operation

                try:
                    logger.debug("Submitting operation #%d with size of %d bytes", sequence_id, len(""))
                    request_ids: Optional[SubmitResponse] = self.submit(operation=run_operation)

                    if request_ids is None or not request_ids.request_ids:
                        raise NeptuneUnexpectedError("Server response is empty")

                    last_request_id = request_ids.request_ids[-1]

                    logger.debug("Operation #%d submitted as %s", sequence_id, last_request_id)
                    self._status_tracking_queue.put(
                        StatusTrackingElement(sequence_id=sequence_id, request_id=last_request_id, timestamp=timestamp)
                    )

                    self._operations_repository.delete_operations(up_to_seq_id=sequence_id)
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

    def close(self) -> None:
        if self._backend is not None:
            self._backend.close()


def _raise_exception(status_code: int) -> None:
    logger.error("HTTP response error: %s", status_code)
    if status_code == 403:
        raise NeptuneUnauthorizedError()
    elif status_code == 408:
        raise NeptuneConnectionLostError()
    elif status_code == 429:
        raise NeptuneTooManyRequestsResponseError()
    elif status_code // 100 == 5:
        raise NeptuneInternalServerError()
    else:
        raise NeptuneUnexpectedResponseError()


def _merge_operations(operations: list[Operation]) -> tuple[UpdateRunSnapshots, SequenceId, datetime.datetime]:
    snapshot_batch = UpdateRunSnapshots(snapshots=[op.operation for op in operations])  # type: ignore
    return snapshot_batch, operations[-1].sequence_id, operations[-1].ts


def _stream_operations(
    operations_repository: OperationsRepository,
    run_id: str,
    project: str,
    max_batch_size: int = 15 * 1024 * 1024,
) -> Generator[tuple[RunOperation, SequenceId, datetime.datetime], None, None]:
    while operations := operations_repository.get_operations(up_to_bytes=max_batch_size):
        if operations[0].operation_type == OperationType.CREATE_RUN:
            create_run = operations.pop(0)
            operation = RunOperation(project=project, run_id=run_id, create=create_run.operation)  # type: ignore
            yield operation, create_run.sequence_id, create_run.ts

            if not operations:
                continue

        data, sequence_id, timestamp = _merge_operations(operations)
        operation = RunOperation(project=project, run_id=run_id, update_batch=data)  # type: ignore
        yield operation, sequence_id, timestamp


class StatusTrackingThread(Daemon):
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

    def close(self) -> None:
        if self._backend is not None:
            self._backend.close()

    def get_next(self) -> Optional[list[StatusTrackingElement]]:
        try:
            return self._status_tracking_queue.peek(max_size=MAX_REQUESTS_STATUS_BATCH_SIZE)
        except queue.Empty:
            return None

    @backoff.on_exception(backoff.expo, NeptuneConnectionLostError, max_time=MAX_REQUEST_RETRY_SECONDS)
    @with_api_errors_handling
    def check_batch(self, *, request_ids: list[str]) -> Optional[BulkRequestStatus]:
        if self._backend is None:
            self._backend = backend_factory(api_token=self._api_token, mode=self._mode)

        response = self._backend.check_batch(request_ids=request_ids, project=self._project)

        status_code = response.status_code

        if status_code != 200:
            _raise_exception(status_code)

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
                        if code_status.code != Code.OK:
                            error = code_to_exception(code_status.detail)
                            self._errors_queue.put(error)

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
                            self._last_ack_timestamp.value = processed_timestamp.timestamp()
                            self._last_ack_timestamp.notify_all()
                else:
                    # Sleep before retry
                    break
        except Exception as e:
            self._errors_queue.put(e)
            self.interrupt()
            self._last_ack_seq.notify_all()
            raise NeptuneSynchronizationStopped() from e
