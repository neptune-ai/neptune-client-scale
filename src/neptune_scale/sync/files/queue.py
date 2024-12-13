import multiprocessing
import pathlib
from typing import (
    NamedTuple,
    Optional,
)

from neptune_scale.util import SharedInt
from neptune_scale.util.abstract import Resource


class UploadMessage(NamedTuple):
    attribute_path: str
    local_path: Optional[pathlib.Path]
    data: Optional[bytes]
    target_path: Optional[str]
    target_basename: Optional[str]


class FileUploadQueue(Resource):
    """Queue for submitting file upload requests from the main process, to a
    FiledUploadWorkerThread, spawned in the worker process.

    The main process submits requests by calling the `submit` method, and waits
    for all uploads to complete by calling the `wait_for_completion` method.
    """

    def __init__(self) -> None:
        self._queue: multiprocessing.Queue[UploadMessage] = multiprocessing.Queue(maxsize=4096)
        self._active_uploads = SharedInt(0)

    @property
    def active_uploads(self) -> int:
        """Returns the number of currently active uploads."""
        with self._active_uploads:
            return self._active_uploads.value

    # Main process API
    def submit(
        self,
        *,
        attribute_path: str,
        local_path: Optional[pathlib.Path],
        data: Optional[bytes],
        target_path: Optional[str],
        target_basename: Optional[str],
    ) -> None:
        assert data is not None or local_path
        with self._active_uploads:
            self._active_uploads.value += 1
            self._queue.put(UploadMessage(attribute_path, local_path, data, target_path, target_basename))

    def wait_for_completion(self, timeout: Optional[float] = None) -> bool:
        """Blocks until all uploads are completed or the timeout is reached.
        Returns True if all uploads completed, False if the timeout was reached.
        """
        with self._active_uploads:
            return self._active_uploads.wait_for(lambda: self._active_uploads.value == 0, timeout=timeout)

    def close(self) -> None:
        self._queue.close()
        self._queue.cancel_join_thread()

    # Worker process API
    def decrement_active(self) -> None:
        with self._active_uploads:
            self._active_uploads.value -= 1
            assert self._active_uploads.value >= 0
            self._active_uploads.notify_all()

    def get(self, timeout: float) -> UploadMessage:
        return self._queue.get(timeout=timeout)
