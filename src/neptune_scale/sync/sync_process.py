from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path

from neptune_api.models import Provider

from neptune_scale.sync.operations_repository import (
    FileUploadRequest,
    Metadata,
    Operation,
    OperationsRepository,
    OperationSubmission,
    OperationType,
    SequenceId,
)
from neptune_scale.sync.size_util import proto_encoded_bytes_field_size
from neptune_scale.sync.storage.azure_storage import upload_to_azure
from neptune_scale.sync.storage.gcs import upload_to_gcp
from neptune_scale.sync.storage.s3 import (
    upload_to_s3_multipart,
    upload_to_s3_single,
)

__all__ = ("run_sync_process",)

import datetime
import functools as ft
import os
import signal
import threading
from collections.abc import Mapping
from types import FrameType
from typing import (
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
    NeptuneFileMetadataExceedsSizeLimit,
    NeptuneFileUploadError,
    NeptuneHistogramBinEdgesContainNaN,
    NeptuneHistogramBinEdgesNotIncreasing,
    NeptuneHistogramTooManyBins,
    NeptuneHistogramValuesLengthMismatch,
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
    FileSignRequest,
    with_api_errors_handling,
)
from neptune_scale.sync.parameters import (
    HTTP_REQUEST_MAX_TIME_SECONDS,
    MAX_REQUEST_SIZE_BYTES,
    MAX_REQUESTS_STATUS_BATCH_SIZE,
    SHUTDOWN_TIMEOUT,
    STATUS_TRACKING_THREAD_SLEEP_TIME,
    SYNC_PROCESS_SLEEP_TIME,
    SYNC_THREAD_SLEEP_TIME,
)
from neptune_scale.util import (
    Daemon,
    envs,
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
    IngestCode.FILE_REF_EXCEEDS_SIZE_LIMIT: NeptuneFileMetadataExceedsSizeLimit,
    IngestCode.HISTOGRAM_BIN_EDGES_CONTAINS_NAN: NeptuneHistogramBinEdgesContainNaN,
    IngestCode.HISTOGRAM_TOO_MANY_BINS: NeptuneHistogramTooManyBins,
    IngestCode.HISTOGRAM_BIN_EDGES_NOT_INCREASING: NeptuneHistogramBinEdgesNotIncreasing,
    IngestCode.HISTOGRAM_VALUES_LENGTH_DOESNT_MATCH_BINS: NeptuneHistogramValuesLengthMismatch,
}


def code_to_exception(code: IngestCode.ValueType) -> Exception:
    if code in CODE_TO_ERROR:
        error = CODE_TO_ERROR[code]
        return error()  # type: ignore
    return NeptuneUnexpectedError(reason=f"Unexpected ingestion error code: {code}")


def run_sync_process(
    operations_repository_path: Path,
    api_token: str,
    project: str,
    family: str,
) -> None:
    logger.info("Data synchronization started")
    stop_event = threading.Event()
    signal.signal(signal.SIGTERM, ft.partial(_handle_stop_signal, stop_event))

    operations_repository = OperationsRepository(db_path=operations_repository_path)
    operations_repository.delete_operation_submissions(up_to_seq_id=None)

    sender_thread = SenderThread(
        api_token=api_token,
        operations_repository=operations_repository,
        family=family,
    )
    status_thread = StatusTrackingThread(
        api_token=api_token,
        project=project,
        operations_repository=operations_repository,
    )
    file_uploader_thread = FileUploaderThread(
        api_token=api_token,
        project=project,
        operations_repository=operations_repository,
    )
    threads = [sender_thread, status_thread, file_uploader_thread]
    parent_process = psutil.Process(os.getpid()).parent()

    def metadata_sending_threads_died() -> bool:
        return not sender_thread.is_alive() or not status_thread.is_alive()

    def interrupt_metadata_sending_threads() -> None:
        if sender_thread.is_alive():
            sender_thread.interrupt()

        # we're interrupting the status thread only if
        # - it's alive, and
        # - the operation statuses in operations repository are empty, and
        # - we know it will stay empty (since sender_thread is dead)
        elif status_thread.is_alive() and operations_repository.get_operation_submission_count(limit=1) == 0:
            status_thread.interrupt()

    def file_uploader_thread_died() -> bool:
        return not file_uploader_thread.is_alive()

    def interrupt_file_uploader_thread() -> None:
        file_uploader_thread.interrupt()

    def all_threads_died() -> bool:
        return all(not t.is_alive() for t in threads)

    def close_all_threads() -> None:
        for t in threads:
            t.interrupt()
        for t in threads:
            t.join(timeout=SHUTDOWN_TIMEOUT)
            t.close()

    for thread in threads:
        thread.start()

    try:
        while not stop_event.is_set():
            for thread in threads:
                thread.join(timeout=SYNC_PROCESS_SLEEP_TIME)

            if metadata_sending_threads_died():
                interrupt_file_uploader_thread()
                interrupt_metadata_sending_threads()

            if file_uploader_thread_died():
                interrupt_metadata_sending_threads()

            if all_threads_died():
                break

            if not _is_process_running(parent_process):
                logger.error("SyncProcess: parent process closed unexpectedly. Exiting")
                break

    except KeyboardInterrupt:
        logger.debug("KeyboardInterrupt received")
    finally:
        logger.info("Data synchronization stopping")
        close_all_threads()
        operations_repository.close(cleanup_files=False)
    logger.info("Data synchronization finished")


def _is_process_running(process: Optional[psutil.Process]) -> bool:
    try:
        # Check if parent exists and is running
        return process is not None and process.is_running()
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return False


def _handle_stop_signal(stop_event: threading.Event, signum: int, frame: Optional[FrameType]) -> None:
    try:
        signal_name = signal.Signals(signum).name
    except ValueError:
        signal_name = str(signum)

    logger.debug(f"Received signal {signal_name}")
    stop_event.set()


class SenderThread(Daemon):
    def __init__(
        self,
        api_token: str,
        family: str,
        operations_repository: OperationsRepository,
    ) -> None:
        super().__init__(name="SenderThread", sleep_time=SYNC_THREAD_SLEEP_TIME)

        self._api_token: str = api_token
        self._family: str = family
        self._operations_repository: OperationsRepository = operations_repository

        queued_range = operations_repository.get_operation_submission_sequence_id_range()
        if queued_range is None:
            last_queued_seq = SequenceId(-1)
        else:
            last_queued_seq = queued_range[1]
        self._last_queued_seq: SequenceId = last_queued_seq

        self._backend: Optional[ApiClient] = None
        self._metadata: Metadata = operations_repository.get_metadata()  # type: ignore

    @backoff.on_exception(backoff.expo, NeptuneRetryableError, max_time=HTTP_REQUEST_MAX_TIME_SECONDS)
    @with_api_errors_handling
    def submit(self, *, operation: RunOperation) -> Optional[SubmitResponse]:
        if self._backend is None:
            self._backend = ApiClient(api_token=self._api_token)

        response = self._backend.submit(operation=operation, family=self._family)

        status_code = response.status_code
        if status_code != 200:
            _raise_exception(status_code)

        return SubmitResponse.FromString(response.content)

    def work(self) -> None:
        try:
            max_operations_size = (
                MAX_REQUEST_SIZE_BYTES
                - len(self._metadata.run_id)
                - len(self._metadata.project)
                - 200  # 200 bytes for RunOperation overhead,
            )

            while operations := self._operations_repository.get_operations(
                from_exclusive=self._last_queued_seq, up_to_bytes=max_operations_size
            ):
                partitioned_operations = _partition_by_type_and_size(
                    operations, self._metadata.run_id, self._metadata.project, max_operations_size
                )

                logger.debug(
                    "Start: submit %d RunOperations. Last queued seq: #%d",
                    len(partitioned_operations),
                    self._last_queued_seq,
                )

                for run_operation, sequence_id, timestamp in partitioned_operations:
                    try:
                        request_ids: Optional[SubmitResponse] = self.submit(operation=run_operation)

                        if request_ids is None or not request_ids.request_ids:
                            raise NeptuneUnexpectedError("Server response is empty")

                        last_request_id = request_ids.request_ids[-1]

                        self._operations_repository.save_operation_submissions(
                            [
                                OperationSubmission(
                                    sequence_id=sequence_id,
                                    request_id=last_request_id,
                                    timestamp=int(timestamp.timestamp() * 1000),
                                )
                            ]
                        )

                        self._last_queued_seq = sequence_id
                    except NeptuneRetryableError as e:
                        self._operations_repository.save_errors([e], sequence_id=sequence_id)
                        # Sleep before retry
                        return

                logger.debug(
                    "Done: submit %d RunOperations. Last queued seq: #%d",
                    len(partitioned_operations),
                    self._last_queued_seq,
                )

        except Exception as e:
            self._operations_repository.save_errors([e])
            self.interrupt()
            raise NeptuneSynchronizationStopped() from e

    def close(self) -> None:
        if self._backend is not None:
            self._backend.close()


def _raise_exception(status_code: int) -> None:
    logger.warning("HTTP response error: %s", status_code)
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

    for op in operations:
        reset_batch = (
            # we don't mix operation types in a single batch
            op.operation_type != batch_type
            # only one CREATE_RUN per batch
            or batch_type == OperationType.CREATE_RUN
            # batch cannot be too big
            or batch_size + proto_encoded_bytes_field_size(op.operation_size_bytes) > max_batch_size
        )
        if reset_batch:
            if batch:
                grouped.append(batch)
            batch = []
            batch_type = op.operation_type
            batch_size = 0

        batch.append(op)
        batch_size += proto_encoded_bytes_field_size(op.operation_size_bytes)

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
    ) -> None:
        super().__init__(name="StatusTrackingThread", sleep_time=STATUS_TRACKING_THREAD_SLEEP_TIME)

        self._api_token: str = api_token
        self._project: str = project
        self._operations_repository: OperationsRepository = operations_repository

        self._backend: Optional[ApiClient] = None

    def close(self) -> None:
        if self._backend is not None:
            self._backend.close()

    @backoff.on_exception(backoff.expo, NeptuneRetryableError, max_time=HTTP_REQUEST_MAX_TIME_SECONDS)
    @with_api_errors_handling
    def check_batch(self, *, request_ids: list[str]) -> Optional[BulkRequestStatus]:
        if self._backend is None:
            self._backend = ApiClient(api_token=self._api_token)

        response = self._backend.check_batch(request_ids=request_ids, project=self._project)

        status_code = response.status_code

        if status_code != 200:
            _raise_exception(status_code)

        return BulkRequestStatus.FromString(response.content)

    def work(self) -> None:
        try:
            while batch := self._operations_repository.get_operation_submissions(limit=MAX_REQUESTS_STATUS_BATCH_SIZE):
                request_ids = [element.request_id for element in batch]
                sequence_ids = [element.sequence_id for element in batch]

                try:
                    response = self.check_batch(request_ids=request_ids)
                    if response is None:
                        raise NeptuneUnexpectedError("Server response is empty")
                except NeptuneRetryableError as e:
                    self._operations_repository.save_errors([e])
                    # Small give up, sleep before retry
                    break

                processed_sequence_id, fatal_sync_error = None, None
                for request_status, request_sequence_id in zip(response.statuses, sequence_ids):
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

                    if errors:
                        self._operations_repository.save_errors(
                            errors=errors,
                            sequence_id=request_sequence_id,
                        )

                    processed_sequence_id = request_sequence_id

                if processed_sequence_id is not None:
                    logger.debug(f"Operations up to #{processed_sequence_id} are completed.")

                    # TODO: delete in a single transaction
                    self._operations_repository.delete_operations(up_to_seq_id=processed_sequence_id)
                    self._operations_repository.delete_operation_submissions(up_to_seq_id=processed_sequence_id)

                if fatal_sync_error is not None:
                    raise fatal_sync_error

                if processed_sequence_id is None:
                    # Sleep before retry
                    break

        except Exception as e:
            self._operations_repository.save_errors([e])
            self.interrupt()
            raise NeptuneSynchronizationStopped() from e

    @staticmethod
    def _is_fatal_error(ex: Exception) -> bool:
        return isinstance(ex, NeptuneProjectError) or isinstance(ex, NeptuneRunError)


class FileUploaderThread(Daemon):
    def __init__(
        self,
        project: str,
        api_token: str,
        operations_repository: OperationsRepository,
        max_concurrent_uploads: Optional[int] = None,
    ) -> None:
        super().__init__(name="FileUploaderThread", sleep_time=1)

        self._project = project
        self._neptune_api_token = api_token
        self._operations_repository = operations_repository
        self._max_concurrent_uploads = max_concurrent_uploads or envs.get_positive_int(
            envs.MAX_CONCURRENT_FILE_UPLOADS, 50
        )
        self._upload_chunk_size = envs.get_positive_int("NEPTUNE_FILE_UPLOAD_CHUNK_SIZE", 16 * 1024 * 1024)
        logger.debug(f"{self._upload_chunk_size=}")
        # This is enforced by GCP, enforcing this for other providers is consistent.
        if self._upload_chunk_size % (256 * 1024) != 0:
            raise ValueError("NEPTUNE_FILE_UPLOAD_CHUNK_SIZE must be a multiple of 256 KiB")

        self._api_client: Optional[ApiClient] = None

        # AIO loop used for file uploads. It needs to be set as current event loop in the actual worker thread,
        # so we do it in work() when also initializing the API client.
        self._aio_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()

    def work(self) -> None:
        try:
            if self._api_client is None:
                self._api_client = ApiClient(api_token=self._neptune_api_token)
                asyncio.set_event_loop(self._aio_loop)

            while file_upload_requests := self._operations_repository.get_file_upload_requests(
                self._max_concurrent_uploads
            ):
                logger.debug(f"Have {len(file_upload_requests)} file upload requests to process")

                file_sign_requests = [
                    FileSignRequest(
                        path=file.destination,
                        size=file.size_bytes,
                        permission="write",
                    )
                    for file in file_upload_requests
                ]
                storage_urls_all = fetch_file_storage_urls(self._api_client, self._project, file_sign_requests)

                # Fan out file uploads as async tasks, and block until all tasks are done.
                # Note that self._upload_file() should not raise an exception, as they are handled
                # in the method itself. However, we still pass return_exceptions=True to make sure asyncio.gather()
                # waits for all the tasks to finish regardless of any exceptions.
                tasks = []
                for file in file_upload_requests:
                    storage_urls = storage_urls_all[file.destination]
                    tasks.append(self._upload_file(file, storage_urls))

                self._aio_loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))

                logger.debug("Upload tasks completed for the current batch")
        except NeptuneRetryableError as e:
            self._operations_repository.save_errors([e])
        except Exception as e:
            logger.error("Fatal error in file uploader thread", exc_info=e)
            self._operations_repository.save_errors([e])
            self.interrupt()
            raise NeptuneSynchronizationStopped() from e

    async def _upload_file(self, file: FileUploadRequest, storage_urls: StorageUrlResult) -> None:
        try:
            provider = storage_urls.provider
            if provider == Provider.AZURE:
                await upload_to_azure(
                    file.source_path, file.mime_type, storage_urls.url, chunk_size=self._upload_chunk_size
                )
            elif provider == Provider.GCP:
                await upload_to_gcp(
                    file.source_path, file.mime_type, storage_urls.url, chunk_size=self._upload_chunk_size
                )
            elif provider == Provider.AWS:
                if storage_urls.multipart_upload is None:
                    await upload_to_s3_single(file.source_path, file.mime_type, storage_urls.url)
                else:
                    multipart = storage_urls.multipart_upload
                    etags = await upload_to_s3_multipart(
                        file.source_path, file.mime_type, multipart.part_size, multipart.part_urls
                    )
                    complete_multipart_upload(
                        self._api_client,
                        project=self._project,
                        upload_id=multipart.upload_id,
                        destination=file.destination,
                        etags=etags,
                    )
            else:
                raise NeptuneUnexpectedError(f"Unsupported file storage provider: {provider}")

            if file.is_temporary:
                logger.debug(f"Removing temporary file {file.source_path}")
                Path(file.source_path).unlink(missing_ok=True)

            self._operations_repository.delete_file_upload_requests([file.sequence_id])  # type: ignore
        except NeptuneRetryableError as e:
            self._operations_repository.save_errors([e])
        except Exception as e:
            # Fatal failure. Do not retry the file, but keep it on disk.
            logger.error(f"Error while uploading file {file.source_path}", exc_info=e)
            self._operations_repository.save_errors([NeptuneFileUploadError()])
            self._operations_repository.delete_file_upload_requests([file.sequence_id])  # type: ignore

    def close(self) -> None:
        if self._api_client is not None:
            self._api_client.close()

        if self._aio_loop is not None:
            self._aio_loop.close()


@dataclass(frozen=True)
class MultipartUpload:
    upload_id: str
    part_size: int
    part_urls: list[str]


@dataclass(frozen=True)
class StorageUrlResult:
    provider: str
    url: str
    multipart_upload: Optional[MultipartUpload]


@backoff.on_exception(backoff.expo, NeptuneRetryableError, max_time=HTTP_REQUEST_MAX_TIME_SECONDS)
@with_api_errors_handling
def fetch_file_storage_urls(
    client: ApiClient, project: str, file_sign_requests: list[FileSignRequest]
) -> dict[str, StorageUrlResult]:
    """Fetch signed URLs for storing files. Returns a dict of target_path -> (provider, upload url)."""

    logger.debug("Fetching file storage urls")
    response = client.fetch_file_storage_urls(file_sign_requests=file_sign_requests, project=project)
    status_code = response.status_code
    if status_code // 100 != 2:
        logger.debug(f"{response.content=!r}")
        _raise_exception(status_code)

    parsed = response.parsed
    if parsed is None:
        raise NeptuneUnexpectedResponseError("Server response is empty")

    results = {}
    for file in parsed.files or []:
        multipart_upload = None
        if file.multipart:
            multipart_upload = MultipartUpload(
                upload_id=file.multipart.upload_id,
                part_size=file.multipart.part_size,
                part_urls=list(file.multipart.part_urls),
            )

        result = StorageUrlResult(provider=str(file.provider), url=file.url, multipart_upload=multipart_upload)
        results[file.path] = result
    return results


@backoff.on_exception(backoff.expo, NeptuneRetryableError, max_time=HTTP_REQUEST_MAX_TIME_SECONDS)
@with_api_errors_handling
def complete_multipart_upload(
    client: ApiClient, project: str, upload_id: str, destination: str, etags: Mapping[int, str]
) -> None:
    """Fetch signed URLs for storing files. Returns a dict of target_path -> (provider, upload url)."""

    logger.debug("Completing multipart upload")
    response = client.complete_multipart_upload(upload_id=upload_id, project=project, path=destination, etags=etags)
    status_code = response.status_code
    if status_code // 100 != 2:
        logger.debug(f"{response.content=!r}")
        _raise_exception(status_code)
