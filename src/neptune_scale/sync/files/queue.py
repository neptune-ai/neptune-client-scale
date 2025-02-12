import io
import multiprocessing
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from neptune_scale.types import File
from neptune_scale.util import SharedInt
from neptune_scale.util.abstract import Resource
from neptune_scale.util.files import FileInfo


@dataclass(frozen=True)
class FileUploadJob:
    # attribute_path: str
    info: FileInfo
    local_path: Optional[Path]
    data_buffer: Optional[io.BytesIO]

    @classmethod
    def from_user_file(cls, attribute_path: str, file: File, info: FileInfo) -> "FileUploadJob":
        if isinstance(file.source, str):
            local_path, data_buffer = Path(file.source), None
        else:
            local_path, data_buffer = None, file.source

        return cls(
            # attribute_path=attribute_path,
            info=info,
            local_path=local_path,
            data_buffer=data_buffer,
        )


@dataclass(frozen=True)
class UploadMessage:
    requests: list[FileUploadJob]


class FileUploadQueue(Resource):
    """Queue for submitting file upload requests from the main process, to a
    FiledUploadWorkerThread, spawned in the worker process.

    The main process submits requests by calling the `submit` method, and waits
    for all uploads to complete by calling the `wait_for_completion` method.
    """

    def __init__(self, max_size: int = 4096) -> None:
        self._queue: multiprocessing.Queue[UploadMessage] = multiprocessing.Queue(maxsize=max_size)
        self._active_uploads = SharedInt(0)

    @property
    def active_uploads(self) -> int:
        """Returns the number of currently active uploads."""
        with self._active_uploads:
            return self._active_uploads.value

    # Main process API
    def submit(
        self,
        files: list[FileUploadJob],
    ) -> None:
        with self._active_uploads:
            self._queue.put(UploadMessage(files))
            self._active_uploads.value += len(files)

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
    def decrement_active(self, num: int = 1) -> None:
        """Signal that `num` uploads have been completed."""

        assert num > 0
        with self._active_uploads:
            assert num <= self._active_uploads.value
            self._active_uploads.value -= num
            self._active_uploads.notify_all()

    def get(self, timeout: float) -> UploadMessage:
        return self._queue.get(timeout=timeout)
