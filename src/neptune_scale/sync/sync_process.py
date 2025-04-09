from __future__ import annotations

import logging
import pathlib
from pathlib import Path

import azure.core.exceptions
from azure.storage.blob import BlobClient

from neptune_scale.sync.operations_repository import (
    FileUploadRequest,
    Metadata,
    Operation,
    OperationsRepository,
    OperationType,
    SequenceId,
)

__all__ = ("SyncProcess",)

import datetime
import functools as ft
import os
import queue
import signal
import threading
from collections.abc import Iterable
from multiprocessing import Process
from types import FrameType
from typing import (
    Generic,
    NamedTuple,
    Optional,
    TypeVar,
)

import backoff
import psutil
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
    NeptuneProjectError,
    NeptuneProjectInvalidName,
    NeptuneProjectNotFound,
    NeptuneRetryableError,
    NeptuneRunConflicting,
    NeptuneRunDuplicate,
    NeptuneRunError,
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
    HTTP_REQUEST_MAX_TIME_SECONDS,
    MAX_REQUEST_SIZE_BYTES,
    MAX_REQUESTS_STATUS_BATCH_SIZE,
    SHUTDOWN_TIMEOUT,
    STATUS_TRACKING_THREAD_SLEEP_TIME,
    SYNC_PROCESS_SLEEP_TIME,
    SYNC_THREAD_SLEEP_TIME,
)
from neptune_scale.sync.util import safe_signal_name
from neptune_scale.util import (
    Daemon,
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
        api_token: str,
        project: str,
        family: str,
        last_queued_seq: SharedInt,
        last_ack_seq: SharedInt,
        last_ack_timestamp: SharedFloat,
    ) -> None:
        super().__init__(name="SyncProcess")

        self._operations_repository_path: Path = operations_repository_path
        self._errors_queue: ErrorsQueue = errors_queue
        self._api_token: str = api_token
        self._project: str = project
        self._family: str = family
        self._last_queued_seq: SharedInt = last_queued_seq
        self._last_ack_seq: SharedInt = last_ack_seq
        self._last_ack_timestamp: SharedFloat = last_ack_timestamp

    def run(self) -> None:
        logger.info("Data synchronization started")
        stop_event = threading.Event()
        signal.signal(signal.SIGTERM, ft.partial(self._handle_stop_signal, stop_event))

        status_tracking_queue: PeekableQueue[StatusTrackingElement] = PeekableQueue()
        operations_repository = OperationsRepository(db_path=self._operations_repository_path)
        sender_thread = SenderThread(
            api_token=self._api_token,
            operations_repository=operations_repository,
            status_tracking_queue=status_tracking_queue,
            errors_queue=self._errors_queue,
            family=self._family,
            last_queued_seq=self._last_queued_seq,
        )
        status_thread = StatusTrackingThread(
            api_token=self._api_token,
            project=self._project,
            operations_repository=operations_repository,
            errors_queue=self._errors_queue,
            status_tracking_queue=status_tracking_queue,
            last_ack_seq=self._last_ack_seq,
            last_ack_timestamp=self._last_ack_timestamp,
        )
        file_uploader_thread = FileUploaderThread(
            api_token=self._api_token,
            project=self._project,
            operations_repository=operations_repository,
            errors_queue=self._errors_queue,
        )
        threads = [sender_thread, status_thread, file_uploader_thread]
        parent_process = psutil.Process(os.getpid()).parent()

        for thread in threads:
            thread.start()

        try:
            sender_thread_error_logged = False
            file_uploader_thread_error_logged = False
            while not stop_event.is_set():
                for thread in threads:
                    thread.join(timeout=SYNC_PROCESS_SLEEP_TIME)

                if not self._is_process_running(parent_process):
                    logger.error("SyncProcess: parent process closed unexpectedly. Exiting")
                    break

                if not sender_thread.is_alive() or not file_uploader_thread.is_alive():
                    logger.debug(
                        f"sender thread alive: {sender_thread.is_alive()}, file uploader thread alive: {file_uploader_thread.is_alive()}"
                    )
                    if status_tracking_queue.peek(max_size=1) is None:
                        logger.error("SyncProcess: sender thread(s) closed unexpectedly. Exiting")
                        break

                    if not sender_thread.is_alive() and not sender_thread_error_logged:
                        logger.error(
                            "SyncProcess: sender thread closed unexpectedly. Waiting for status tracking thread to finish"
                        )
                        sender_thread_error_logged = True

                    if not file_uploader_thread.is_alive() and not file_uploader_thread_error_logged:
                        logger.error(
                            "SyncProcess: file uploader thread closed unexpectedly. Waiting for status tracking thread to finish"
                        )
                        file_uploader_thread_error_logged = True

                if not status_thread.is_alive():
                    logger.error("SyncProcess: status tracking thread closed unexpectedly. Exiting")
                    break

        except KeyboardInterrupt:
            logger.debug("KeyboardInterrupt received")
        finally:
            logger.info("Data synchronization stopping")
            for thread in threads:
                thread.interrupt()

            for thread in threads:
                thread.join(timeout=SHUTDOWN_TIMEOUT)
                thread.close()  # type: ignore
            operations_repository.close(cleanup_files=False)
        logger.info("Data synchronization finished")

    @staticmethod
    def _is_process_running(process: Optional[psutil.Process]) -> bool:
        try:
            # Check if parent exists and is running
            return process is not None and process.is_running()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return False

    @staticmethod
    def _handle_stop_signal(stop_event: threading.Event, signum: int, frame: Optional[FrameType]) -> None:
        logger.debug(f"Received signal {safe_signal_name(signum)}")
        stop_event.set()


class SenderThread(Daemon):
    def __init__(
        self,
        api_token: str,
        family: str,
        operations_repository: OperationsRepository,
        status_tracking_queue: PeekableQueue[StatusTrackingElement],
        errors_queue: ErrorsQueue,
        last_queued_seq: SharedInt,
    ) -> None:
        super().__init__(name="SenderThread", sleep_time=SYNC_THREAD_SLEEP_TIME)

        self._api_token: str = api_token
        self._family: str = family
        self._operations_repository: OperationsRepository = operations_repository
        self._status_tracking_queue: PeekableQueue[StatusTrackingElement] = status_tracking_queue
        self._errors_queue: ErrorsQueue = errors_queue
        self._last_queued_seq: SharedInt = last_queued_seq

        self._backend: Optional[ApiClient] = None
        self._metadata: Metadata = operations_repository.get_metadata()  # type: ignore

    @backoff.on_exception(backoff.expo, NeptuneRetryableError, max_time=HTTP_REQUEST_MAX_TIME_SECONDS)
    @with_api_errors_handling
    def submit(self, *, operation: RunOperation) -> Optional[SubmitResponse]:
        if self._backend is None:
            self._backend = backend_factory(api_token=self._api_token)

        response = self._backend.submit(operation=operation, family=self._family)

        status_code = response.status_code
        if status_code != 200:
            _raise_exception(status_code)

        return response.parsed

    def work(self) -> None:
        try:
            max_operations_size = (
                MAX_REQUEST_SIZE_BYTES
                - len(self._metadata.run_id)
                - len(self._metadata.project)
                - 200  # 200 bytes for RunOperation overhead,
            )
            with self._last_queued_seq:
                sequence_id = SequenceId(self._last_queued_seq.value)

            while operations := self._operations_repository.get_operations(
                from_exclusive=sequence_id, up_to_bytes=max_operations_size
            ):
                partitioned_operations = _partition_by_type_and_size(
                    operations, self._metadata.run_id, self._metadata.project, max_operations_size
                )

                logger.debug(
                    "Start: submit %d RunOperations. Last queued seq: #%d",
                    len(partitioned_operations),
                    sequence_id,
                )

                for run_operation, sequence_id, timestamp in partitioned_operations:
                    try:
                        request_ids: Optional[SubmitResponse] = self.submit(operation=run_operation)

                        if request_ids is None or not request_ids.request_ids:
                            raise NeptuneUnexpectedError("Server response is empty")

                        last_request_id = request_ids.request_ids[-1]

                        self._status_tracking_queue.put(
                            StatusTrackingElement(
                                sequence_id=sequence_id, request_id=last_request_id, timestamp=timestamp
                            )
                        )

                        # Update Last PUT sequence id and notify threads in the main process
                        with self._last_queued_seq:
                            self._last_queued_seq.value = sequence_id
                            self._last_queued_seq.notify_all()
                    except NeptuneRetryableError as e:
                        self._errors_queue.put(e)
                        # Sleep before retry
                        return

                if logger.isEnabledFor(logging.DEBUG):
                    # Don't access multiprocessing.Value if not in debug mode
                    logger.debug(
                        "Done: submit %d RunOperations. Last queued seq: #%d",
                        len(partitioned_operations),
                        self._last_queued_seq.value,
                    )

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


def _partition_by_type_and_size(
    operations: list[Operation], run_id: str, project: str, max_batch_size: int
) -> list[tuple[RunOperation, SequenceId, datetime.datetime]]:
    grouped: list[list[Operation]] = []
    batch: list[Operation] = []
    batch_type: Optional[OperationType] = None
    batch_size = 0

    def op_size_with_overhead(_op: Operation) -> int:
        return _op.operation_size_bytes + 5  # 1 byte for wire type, 4 bytes for size (2mb)

    for op in operations:
        reset_batch = (
            # we don't mix operation types in a single batch
            op.operation_type != batch_type
            # only one CREATE_RUN per batch
            or batch_type == OperationType.CREATE_RUN
            # batch cannot be too big
            or batch_size + op_size_with_overhead(op) > max_batch_size
        )
        if reset_batch:
            if batch:
                grouped.append(batch)
            batch = []
            batch_type = op.operation_type
            batch_size = 0

        batch.append(op)
        batch_size += op_size_with_overhead(op)

    if batch:
        grouped.append(batch)

    def to_run_operation(ops: list[Operation]) -> tuple[RunOperation, SequenceId, datetime.datetime]:
        if ops[0].operation_type == OperationType.CREATE_RUN:
            return (
                RunOperation(project=project, run_id=run_id, create=ops[0].operation),
                ops[-1].sequence_id,
                ops[-1].ts,
            )
        else:
            snapshots = UpdateRunSnapshots(snapshots=[_op.operation for _op in ops])
            return RunOperation(project=project, run_id=run_id, update_batch=snapshots), ops[-1].sequence_id, ops[-1].ts

    return [(to_run_operation(ops)) for ops in grouped]


class StatusTrackingThread(Daemon):
    def __init__(
        self,
        api_token: str,
        project: str,
        operations_repository: OperationsRepository,
        errors_queue: ErrorsQueue,
        status_tracking_queue: PeekableQueue[StatusTrackingElement],
        last_ack_seq: SharedInt,
        last_ack_timestamp: SharedFloat,
    ) -> None:
        super().__init__(name="StatusTrackingThread", sleep_time=STATUS_TRACKING_THREAD_SLEEP_TIME)

        self._api_token: str = api_token
        self._project: str = project
        self._operations_repository: OperationsRepository = operations_repository
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

    @backoff.on_exception(backoff.expo, NeptuneRetryableError, max_time=HTTP_REQUEST_MAX_TIME_SECONDS)
    @with_api_errors_handling
    def check_batch(self, *, request_ids: list[str]) -> Optional[BulkRequestStatus]:
        if self._backend is None:
            self._backend = backend_factory(api_token=self._api_token)

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

                operations_to_commit, processed_sequence_id, processed_timestamp, fatal_sync_error = 0, None, None, None
                for request_status, request_sequence_id, timestamp in zip(response.statuses, sequence_ids, timestamps):
                    if any(code_status.code == Code.UNAVAILABLE for code_status in request_status.code_by_count):
                        logger.debug(f"Operation #{request_sequence_id} is not yet processed.")
                        # Request status not ready yet, sleep and retry
                        break

                    errors = [
                        code_to_exception(status.detail)
                        for status in request_status.code_by_count
                        if status.code != Code.OK
                    ]

                    fatal_sync_error = next(filter(self._is_fatal_error, errors), None)
                    if fatal_sync_error is not None:
                        break

                    for error in errors:
                        self._errors_queue.put(error)

                    operations_to_commit += 1
                    processed_sequence_id, processed_timestamp = request_sequence_id, timestamp

                if operations_to_commit > 0:
                    self._status_tracking_queue.commit(operations_to_commit)

                    # Update Last ACK sequence id and notify threads in the main process
                    if processed_sequence_id is not None:
                        logger.debug(f"Operations up to #{processed_sequence_id} are completed.")

                        self._operations_repository.delete_operations(up_to_seq_id=processed_sequence_id)

                        with self._last_ack_seq:
                            self._last_ack_seq.value = processed_sequence_id
                            self._last_ack_seq.notify_all()

                    # Update Last ACK timestamp and notify threads in the main process
                    if processed_timestamp is not None:
                        with self._last_ack_timestamp:
                            self._last_ack_timestamp.value = processed_timestamp.timestamp()
                            self._last_ack_timestamp.notify_all()

                if fatal_sync_error is not None:
                    raise fatal_sync_error

                if operations_to_commit == 0:
                    # Sleep before retry
                    break

        except Exception as e:
            self._errors_queue.put(e)
            self.interrupt()
            self._last_ack_seq.notify_all()
            raise NeptuneSynchronizationStopped() from e

    @staticmethod
    def _is_fatal_error(ex: Exception) -> bool:
        return isinstance(ex, NeptuneProjectError) or isinstance(ex, NeptuneRunError)


class FileUploaderThread(Daemon):
    def __init__(
        self, project: str, api_token: str, operations_repository: OperationsRepository, errors_queue: ErrorsQueue
    ) -> None:
        super().__init__(name="FileUploaderThread", sleep_time=1)

        self._project = project
        self._neptune_api_token = api_token
        self._operations_repository = operations_repository
        self._errors_queue = errors_queue

        self._neptune_client: Optional[ApiClient] = None

    def close(self) -> None:
        if self._neptune_client is not None:
            self._neptune_client.close()

    def work(self) -> None:
        try:
            while file_upload_requests := self._operations_repository.get_file_upload_requests(10):
                logger.debug(f"Have {len(file_upload_requests)} file upload requests to process")

                if not self._neptune_client:
                    self._neptune_client = backend_factory(self._neptune_api_token)

                target_paths = [file.target_path for file in file_upload_requests]

                try:
                    storage_urls = fetch_file_storage_urls(self._neptune_client, self._project, target_paths)
                except NeptuneRetryableError as e:
                    logger.debug(f"Error while fetching file storage urls: {e}")
                    self._errors_queue.put(e)
                    return

                for file in file_upload_requests:
                    try:
                        upload_file(file.source_path, file.mime_type, file.size_bytes, storage_urls[file.target_path])
                    except azure.core.exceptions.AzureError as e:
                        logger.warning(f"Will retry uploading file {file.source_path}: {e}")
                        return
                    except Exception as e:
                        # Fatal failure. Do not retry the file, but clean it up and continue with the next one
                        self._errors_queue.put(e)
                        logger.error(f"Error while uploading file {file.source_path}", exc_info=e)

                    self._finalize_upload(file)
        except Exception as e:
            self._errors_queue.put(e)
            self.interrupt()
            raise NeptuneSynchronizationStopped() from e

    def _finalize_upload(self, file: FileUploadRequest) -> None:
        assert file.sequence_id is not None

        self._operations_repository.delete_file_upload_requests([file.sequence_id])
        try:
            if file.is_temporary:
                logger.debug(f"Removing temporary file {file.source_path}")
                pathlib.Path(file.source_path).unlink(missing_ok=True)
        except Exception as e:
            logger.warning(f"Unable to remove temporary file {file.source_path}: {e}")
            self._errors_queue.put(e)


@backoff.on_exception(backoff.expo, NeptuneRetryableError, max_time=HTTP_REQUEST_MAX_TIME_SECONDS)
@with_api_errors_handling
def fetch_file_storage_urls(client: ApiClient, project: str, target_paths: Iterable[str]) -> dict[str, str]:
    """Fetch Azure urls for storing files. Return a dict of target_path -> upload url"""
    response = client.fetch_file_storage_urls(paths=target_paths, project=project, mode="write")
    status_code = response.status_code
    if status_code != 200:
        _raise_exception(status_code)

    if response.parsed is None:
        raise NeptuneUnexpectedError("Server response is empty")

    return {file.path: file.url for file in response.parsed.files}


def upload_file(local_path: str, mime_type: str, size_bytes: int, storage_url: str) -> None:
    logger.debug(f"Start: upload file {local_path}")

    with open(local_path, "rb") as file:
        client = BlobClient.from_blob_url(storage_url, initial_backoff=5, increment_base=3, retry_total=5)

        # Note that max_concurrency and chunking will only apply if the file is larger than 64MB (Azure default).
        # Otherwise, it will be uploaded in a single request, single thread.
        client.upload_blob(
            file,
            content_type=mime_type,
            overwrite=True,
            max_concurrency=8,  # The default is 16, set it lower to save threads
            length=size_bytes,
        )

    logger.debug(f"Done: upload file {local_path}")
