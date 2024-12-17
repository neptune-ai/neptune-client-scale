import io
import mimetypes
import time
import uuid
from concurrent import futures
from datetime import datetime
from pathlib import Path
from queue import Empty
from typing import (
    BinaryIO,
    Callable,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
)

import backoff
from neptune_api.proto.google_rpc.code_pb2 import Code
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
    UpdateRunSnapshot,
    Value,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation
from neptune_api.types import Response

from neptune_scale.exceptions import (
    NeptuneInternalServerError,
    NeptuneRetryableError,
    NeptuneUnauthorizedError,
    NeptuneUnexpectedResponseError,
)
from neptune_scale.net.api_client import (
    ApiClient,
    backend_factory,
    with_api_errors_handling,
)
from neptune_scale.net.ingest_code import code_to_exception
from neptune_scale.net.serialization import datetime_to_proto
from neptune_scale.sync.errors_tracking import ErrorsQueue
from neptune_scale.sync.files.queue import (
    FileUploadQueue,
    UploadMessage,
)
from neptune_scale.sync.parameters import MAX_REQUEST_RETRY_SECONDS
from neptune_scale.util import (
    Daemon,
    get_logger,
)
from neptune_scale.util.abstract import Resource

logger = get_logger()


class FileUploadWorkerThread(Daemon, Resource):
    """Consumes messages from the provided FileUploadQueue and performs the upload operation
    in a pool of worker threads.
    """

    def __init__(
        self,
        *,
        project: str,
        run_id: str,
        api_token: str,
        family: str,
        mode: Literal["async", "disabled"],
        input_queue: FileUploadQueue,
        errors_queue: ErrorsQueue,
    ) -> None:
        super().__init__(sleep_time=0.5, name="FileUploader")

        self._project = project
        self._run_id = run_id
        self._api_token = api_token
        self._family = family
        self._mode = mode
        self._input_queue = input_queue
        self._errors_queue = errors_queue
        self._executor = futures.ThreadPoolExecutor()

        self._backend: Optional[ApiClient] = None

    def work(self) -> None:
        while self.is_running():
            try:
                msg = self._input_queue.get(timeout=1)
            except Empty:
                continue

            try:
                if self._backend is None:
                    self._backend = backend_factory(self._api_token, self._mode)

                future = self._executor.submit(
                    self._do_upload,
                    msg.timestamp,
                    msg.attribute_path,
                    msg.local_path,
                    msg.data,
                    msg.target_path,
                    msg.target_basename,
                )
                future.add_done_callback(self._make_done_callback(msg))
            except Exception as e:
                logger.error(f"Failed to submit file upload task for `{msg.attribute_path}`: {e}")
                self._errors_queue.put(e)

    def close(self) -> None:
        self._executor.shutdown()

    def _do_upload(
        self,
        timestamp: datetime,
        attribute_path: str,
        local_path: Optional[Path],
        data: Optional[bytes],
        target_path: Optional[str],
        target_basename: Optional[str],
    ) -> None:
        path, mime_type = determine_path_and_mime_type(
            self._run_id, attribute_path, local_path, target_path, target_basename
        )

        try:
            url = self._request_upload_url(path)
            src = local_path.open("rb") if local_path else io.BytesIO(data)  # type: ignore
            upload_file(src, url, mime_type)

            request_id = self._submit_attribute(attribute_path, path, timestamp)
            self._wait_for_completion(request_id)
        except Exception as e:
            raise e

    @backoff.on_exception(backoff.expo, NeptuneRetryableError, max_time=MAX_REQUEST_RETRY_SECONDS)
    @with_api_errors_handling
    def _request_upload_url(self, file_path: str) -> str:
        assert self._backend is not None
        return self._backend.fetch_file_storage_info(self._project, file_path, "write")

    @backoff.on_exception(backoff.expo, NeptuneRetryableError, max_time=MAX_REQUEST_RETRY_SECONDS)
    @with_api_errors_handling
    def _submit_attribute(self, attribute_path: str, file_path: str, timestamp: datetime) -> Sequence[str]:
        """Request the Ingest API to save a File type attribute under `attribute_path`.
        Returns a request id for tracking the status of the operation.
        """

        assert self._backend is not None  # mypy

        op = RunOperation(
            project=self._project,
            run_id=self._run_id,
            # TODO: replace with the actual Value type once it's introduced to protobuf
            update=UpdateRunSnapshot(
                timestamp=datetime_to_proto(timestamp), assign={attribute_path: Value(string=file_path)}
            ),
        )

        response = self._backend.submit(operation=op, family=self._family)
        raise_on_response(response)
        assert response.parsed  # mypy

        return response.parsed.request_ids

    @backoff.on_exception(backoff.expo, NeptuneRetryableError, max_time=MAX_REQUEST_RETRY_SECONDS)
    @with_api_errors_handling
    def _wait_for_completion(self, request_ids: List[str]) -> None:
        assert self._backend is not None  # mypy

        while self.is_running():
            response = self._backend.check_batch(request_ids, self._project)
            raise_on_response(response)
            assert response.parsed  # mypy

            status = response.parsed.statuses[0]
            if any(code_status.code == Code.UNAVAILABLE for code_status in status.code_by_count):
                # The request is still being processed, check back in a moment
                time.sleep(1)
                continue

            for code_status in status.code_by_count:
                if code_status.code != Code.OK:
                    exc_class = code_to_exception(code_status.detail)
                    self._errors_queue.put(exc_class())

            # The request finished successfully or with an error, either way we can break
            break

    def _make_done_callback(self, message: UploadMessage) -> Callable[[futures.Future], None]:
        """Returns a callback function suitable for use with Future.add_done_callback(). Decreases the active upload
        count and propagates any exception to the errors queue.
        """

        def _on_task_completed(future: futures.Future) -> None:
            self._input_queue.decrement_active()

            try:
                future.result()
            except Exception as e:
                logger.error(f"Failed to upload file as `{message.attribute_path}`: {e}")
                self._errors_queue.put(e)

        return _on_task_completed


def determine_path_and_mime_type(
    run_id: str,
    attribute_path: str,
    local_path: Optional[Path],
    target_path: Optional[str],
    target_basename: Optional[str],
) -> Tuple[str, str]:
    mime_type = guess_mime_type(attribute_path, local_path)

    # Target path always takes precedence as-is
    if target_path:
        return target_path, mime_type

    if local_path:
        local_basename = local_path.name
    else:
        local_basename = f"{uuid.uuid4()}{mimetypes.guess_extension(mime_type)}"

    if target_basename:
        parts: tuple[str, ...] = (run_id, attribute_path, target_basename)
    else:
        parts = (run_id, attribute_path, str(uuid.uuid4()), local_basename)

    return "/".join(parts), mime_type


def upload_file(source: BinaryIO, url: str, mime_type: str) -> None:
    # TODO: do the actual work :)
    assert source and url and mime_type
    time.sleep(1)
    pass


def guess_mime_type(attribute_path: str, local_path: Optional[Path]) -> str:
    if local_path:
        mime_type, _ = mimetypes.guess_type(local_path or attribute_path)
        if mime_type is not None:
            return mime_type

    mime_type, _ = mimetypes.guess_type(attribute_path)
    return mime_type or "application/octet-stream"


def raise_on_response(response: Response, allow_empty_response: bool = False) -> None:
    if response.status_code == 200:
        return

    if response.parsed is None and not allow_empty_response:
        raise NeptuneUnexpectedResponseError(reason="Empty server response")

    if response.status_code == 403:
        raise NeptuneUnauthorizedError()

    logger.error("HTTP response error: %s", response.status_code)
    if response.status_code // 100 == 5:
        raise NeptuneInternalServerError()
    else:
        raise NeptuneUnexpectedResponseError()
