import io
import mimetypes
import time
import uuid
from concurrent import futures
from pathlib import Path
from queue import Empty
from typing import (
    BinaryIO,
    Callable,
    Optional,
    Tuple,
)

from neptune_scale.exceptions import NeptuneScaleError
from neptune_scale.sync.errors_tracking import ErrorsQueue
from neptune_scale.sync.files.queue import (
    FileUploadQueue,
    UploadMessage,
)
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
        input_queue: FileUploadQueue,
        errors_queue: ErrorsQueue,
    ) -> None:
        super().__init__(sleep_time=0.5, name="FileUploader")

        self._project = project
        self._run_id = run_id
        self._api_token = api_token
        self._family = family
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
                future = self._executor.submit(
                    self._do_upload, msg.attribute_path, msg.local_path, msg.data, msg.target_path, msg.target_basename
                )
                future.add_done_callback(self._make_done_callback(msg))
            except Exception as e:
                logger.error(f"Failed to submit file upload task for `{msg.local_path}` as `{msg.attribute_path}`: {e}")
                self._errors_queue.put(e)

    def close(self) -> None:
        self._executor.shutdown()

    def _do_upload(
        self,
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
            url = self._request_upload_url(attribute_path, path)
            src = local_path.open("rb") if local_path else io.BytesIO(data)  # type: ignore
            upload_file(src, url, mime_type)
            self._finalize_upload(path)
        except Exception as e:
            self._finalize_upload(path, e)
            raise e

    def _request_upload_url(self, attribute_path: str, file_path: str) -> str:
        assert self._api_token
        # TODO: Make this retryable
        time.sleep(0.2)
        return ".".join(["http://localhost:8012/", attribute_path, file_path])

    def _finalize_upload(self, attribute_path: str, error: Optional[Exception] = None) -> None:
        """Notify the backend that the upload process is complete successfully or with an error."""
        # TODO: hit the backend, needs to be retryable
        print(f"finalizing file {attribute_path}")
        time.sleep(1)
        print(f"finalized file {attribute_path}")

    def _make_done_callback(self, message: UploadMessage) -> Callable[[futures.Future], None]:
        """Returns a callback function suitable for use with Future.add_done_callback(). Decreases the active upload
        count and propagates any exception to the errors queue.
        """

        def _on_task_completed(future: futures.Future) -> None:
            self._input_queue.decrement_active()

            exc = future.exception()
            if future.cancelled() and exc is None:
                exc = NeptuneScaleError("Operation cancelled")

            if exc:
                logger.error(f"Failed to upload file `{message.local_path}` as `{message.attribute_path}`: {exc}")
                self._errors_queue.put(exc)

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
