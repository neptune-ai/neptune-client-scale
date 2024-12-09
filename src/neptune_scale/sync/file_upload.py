from concurrent import futures
from pathlib import Path
from typing import (
    Optional,
    Protocol,
)

from neptune_scale.sync.errors_tracking import ErrorsQueue
from neptune_scale.util import SharedInt


class Finalizer(Protocol):
    def __call__(self, path: str, error: Optional[Exception] = None) -> None: ...


class FileUploader:
    def __init__(
        self, project: str, api_token: str, family: str, in_progress_counter: SharedInt, errors_queue: ErrorsQueue
    ) -> None:
        self._project = project
        self._api_token = api_token
        self._family = family
        self._errors_queue = errors_queue
        self._in_progress_counter = in_progress_counter
        self._executor = futures.ThreadPoolExecutor()

    def start_upload(
        self,
        *,
        finalizer: Finalizer,
        attribute_path: str,
        local_path: Path,
        target_path: Optional[str],
        target_basename: Optional[str],
    ) -> None:
        with self._in_progress_counter:
            self._in_progress_counter.value += 1

        self._executor.submit(self._do_upload, finalizer, attribute_path, local_path, target_path, target_basename)

    def _do_upload(
        self,
        finalizer: Finalizer,
        attribute_path: str,
        local_path: Path,
        target_path: Optional[str],
        target_basename: Optional[str],
    ) -> None:
        path = determine_path(local_path, target_path, target_basename)

        try:
            url = self._request_upload_url(attribute_path, path)
            upload_file(local_path, url)
            error = None
        except Exception as e:
            error = e

        finalizer(path, error=error)

        with self._in_progress_counter:
            self._in_progress_counter.value -= 1
            assert self._in_progress_counter.value >= 0

            self._in_progress_counter.notify_all()

    def _request_upload_url(self, attribute_path: str, file_path: str) -> str:
        assert self._api_token
        # TODO: temporary
        return ".".join(["http://localhost:8012/", attribute_path, file_path])

    def wait_for_completion(self) -> None:
        self._executor.shutdown()
        with self._in_progress_counter:
            assert self._in_progress_counter.value >= 0


def determine_path(local_path: Path, target_path: Optional[str], target_basename: Optional[str]) -> str:
    if target_path:
        return target_path

    # TODO: figure out the path
    return str(Path("DUMMY_PATH") / local_path)


def upload_file(local_path: Path, url: str) -> None:
    # TODO: do the actual work :)
    assert local_path and url
    pass
