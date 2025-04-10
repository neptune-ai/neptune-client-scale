import mimetypes
import pathlib
from typing import (
    IO,
    Optional,
    Union,
    cast,
)
from collections.abc import Iterable

import filetype

from neptune_scale.api.metrics import File
from neptune_scale.sync.operations_repository import FileUploadRequest
from neptune_scale.util import get_logger

DEFAULT_MIME_TYPE = "application/octet-stream"
logger = get_logger()


def guess_mime_type_from_file(local_path: Union[pathlib.Path, str], target_path: str) -> Optional[str]:
    """Guess mime type by local file path and the target path. In case of an error, return None.

    We return None instead of DEFAULT_MIME_TYPE under the assumption that an error here also means
    that the file is inaccessible or not found, thus the upload will fail.
    """
    try:
        if mime := mimetypes.guess_type(local_path)[0]:
            return mime

        if mime := mimetypes.guess_type(target_path)[0]:
            return mime

        if mime := filetype.guess_mime(local_path):
            return cast(str, mime)

        return DEFAULT_MIME_TYPE
    except Exception as e:
        logger.error(f"Error determining mime type for {target_path}: {e}")
        return None


def guess_mime_type_from_bytes(data: bytes, target_path: str) -> str:
    """Guess mime type by providing a buffer and the target path"""
    try:
        if mime := mimetypes.guess_type(target_path)[0]:
            return mime

        if mime := filetype.guess_mime(data):
            return cast(str, mime)

        return DEFAULT_MIME_TYPE
    except Exception as e:
        logger.warning(f"Error determining mime type for {target_path}, defaulting to ${DEFAULT_MIME_TYPE}: {e}")
        return DEFAULT_MIME_TYPE


def user_files_to_upload_requests(
    files: dict[str, Iterable[Union[str, pathlib.Path, IO[bytes], File]]],
) -> dict[str, FileUploadRequest]:
    # 1. validate value types in dict
    #    - raise ValueError just as Run._log() does
    #    - DO NOT check target_path length if provided - leave it to MetadataSplitter
    #    - ditto for mime type
    # 2. for each entry
    #    - get mime type
    #    - get file size
    #    - generate target_path (keep to the 4kiB limit)
    #    - create upload request
    #
    #  Error handling: drop any files that raise errors and emit a warning, eg. any FileNotFoundError,
    #       PermissionDenied -- silence all exceptions
    #  Rationale: don't break training on disk errors, esp. when we'll be creating temporary files -> out of disk space
    #    should not stop the training
    # The resulting dict might thus be empty
    #
    # Alternatively we could wrap any error in eg. NeptuneUnableToLogData and raise it. Or ValueError even.
    return {}
