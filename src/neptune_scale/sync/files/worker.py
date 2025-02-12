import typing
from collections.abc import Iterable
from concurrent import futures
from queue import Empty
from typing import (
    Callable,
    Optional,
)

import backoff
import httpx

from neptune_scale.exceptions import (
    NeptuneRetryableError,
    NeptuneUnexpectedError,
)
from neptune_scale.net.api_client import (
    ApiClient,
    backend_factory,
    raise_for_http_status,
    with_api_errors_handling,
)
from neptune_scale.sync.errors_tracking import ErrorsQueue
from neptune_scale.sync.files.queue import (
    FileUploadJob,
    FileUploadQueue,
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
        neptune_api_token: str,
        input_queue: FileUploadQueue,
        errors_queue: ErrorsQueue,
    ) -> None:
        super().__init__(sleep_time=0.5, name="FileUploader")

        self._project = project
        self._neptune_api_token = neptune_api_token
        self._client: Optional[ApiClient] = None
        self._input_queue = input_queue
        self._errors_queue = errors_queue
        self._executor = futures.ThreadPoolExecutor()

    def work(self) -> None:
        while True:
            try:
                msg = self._input_queue.get(timeout=0.5)
            except Empty:
                return

            try:
                if not self._client:
                    self._client = backend_factory(self._neptune_api_token, mode="async")

                paths = [request.info.path for request in msg.requests]
                storage_urls = fetch_file_storage_urls(self._client, self._project, paths)
            except Exception as e:
                logger.error(f"Failed to retrieve storage information for upload of {len(msg.requests)} files: {e}")
                self._input_queue.decrement_active(len(msg.requests))
                self._errors_queue.put(e)
                continue

            for request in msg.requests:
                try:
                    future = self._executor.submit(_do_upload, request, storage_urls[request.info.path])
                    future.add_done_callback(self._make_done_callback(request))
                except Exception as e:
                    logger.error(f"Failed to submit file upload task for `{request.info.path}`: {e}")
                    self._input_queue.decrement_active()
                    self._errors_queue.put(e)

    def close(self) -> None:
        self._executor.shutdown()

    def _make_done_callback(self, request: FileUploadJob) -> Callable[[futures.Future], None]:
        """Returns a callback function suitable for use with Future.add_done_callback(). Decreases the active upload
        count and propagates any exception to the errors queue.
        """

        def _on_task_completed(future: futures.Future) -> None:
            self._input_queue.decrement_active()

            try:
                future.result()
            except Exception as e:
                logger.error(f"Failed to upload file as `{request.info.path}`: {e}")
                self._errors_queue.put(e)

        return _on_task_completed


@backoff.on_exception(backoff.expo, NeptuneRetryableError, max_time=MAX_REQUEST_RETRY_SECONDS)
@with_api_errors_handling
def fetch_file_storage_urls(client: ApiClient, project: str, paths: Iterable[str]) -> dict[str, str]:
    response = client.fetch_file_storage_urls(paths=paths, project=project, mode="write")
    status_code = response.status_code
    if status_code != 200:
        raise_for_http_status(status_code)

    if response.parsed is None:
        raise NeptuneUnexpectedError("Server response is empty")

    return {file.path: file.url for file in response.parsed.files}


def _do_upload(request: FileUploadJob, storage_url: str) -> None:
    # TODO: replace with Azure SDK
    def upload_to_storage(data: typing.BinaryIO) -> None:
        response = httpx.put(storage_url, content=data, headers={"x-ms-blob-type": "BlockBlob"}, verify=False)
        response.raise_for_status()

    if request.local_path:
        with open(request.local_path, "rb") as f:
            upload_to_storage(f)
    else:
        assert request.data_buffer is not None  # mypy
        upload_to_storage(request.data_buffer)
