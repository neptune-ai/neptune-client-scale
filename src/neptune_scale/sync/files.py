import hashlib
import mimetypes
import pathlib
from typing import (
    Optional,
    Union,
    cast,
)

import filetype

from neptune_scale.sync.parameters import MAX_FILE_DESTINATION_LENGTH
from neptune_scale.util import get_logger

__all__ = (
    "guess_mime_type_from_file",
    "guess_mime_type_from_bytes",
    "generate_destination",
)

DEFAULT_MIME_TYPE = "application/octet-stream"
logger = get_logger()


def guess_mime_type_from_file(local_path: Union[pathlib.Path, str], destination: Optional[str] = None) -> Optional[str]:
    """Guess mime type by local file path and the destination path. In case of an error, return None.

    We return None instead of DEFAULT_MIME_TYPE under the assumption that an error here also means
    that the file is inaccessible or not found, thus the upload will fail.
    """
    try:
        if mime := mimetypes.guess_type(local_path)[0]:
            return mime

        if destination:
            if mime := mimetypes.guess_type(destination)[0]:
                return mime

        if mime := filetype.guess_mime(local_path):
            return cast(str, mime)

        return DEFAULT_MIME_TYPE
    except Exception as e:
        logger.warning(f"Error determining mime type for {local_path}: {e}")
        return None


def guess_mime_type_from_bytes(data: bytes, destination: Optional[str] = None) -> str:
    """Guess mime type by providing a buffer and the target path"""
    try:
        if destination:
            if mime := mimetypes.guess_type(destination)[0]:
                return mime

        if mime := filetype.guess_mime(data):
            return cast(str, mime)

        return DEFAULT_MIME_TYPE
    except Exception as e:
        logger.warning(f"Error determining mime type for the provided buffer, defaulting to ${DEFAULT_MIME_TYPE}: {e}")
        return DEFAULT_MIME_TYPE


# Maximum lengths of various components of the file destination.
MAX_RUN_ID_COMPONENT_LENGTH = 300
MAX_ATTRIBUTE_PATH_COMPONENT_LENGTH = 300
MAX_FILENAME_EXTENSION_LENGTH = 18
MAX_FILENAME_PATH_COMPONENT_LENGTH = 180

# Format: "run_id/attribute_path/file.txt", we need +2 to account for the "/" separators.
# The lengths should add up to MAX_FILE_DESTINATION_LENGTH.
assert (
    MAX_RUN_ID_COMPONENT_LENGTH
    + MAX_ATTRIBUTE_PATH_COMPONENT_LENGTH
    + MAX_FILENAME_EXTENSION_LENGTH
    + MAX_FILENAME_PATH_COMPONENT_LENGTH
    + 2
) == MAX_FILE_DESTINATION_LENGTH


def _digest_suffix(string: str) -> str:
    digest = hashlib.blake2b(string.encode("utf-8"), digest_size=8).hexdigest()
    return "-" + digest


def _trim_with_optional_digest_suffix(input_str: str, max_length: int) -> str:
    if len(input_str) <= max_length:
        return input_str

    suffix = _digest_suffix(input_str)
    return input_str[: max_length - len(suffix)] + suffix


def _trim_and_sanitize_with_digest_suffix(input_str: str, max_length: int) -> str:
    sanitized = input_str
    if "/" in input_str:
        sanitized = input_str.replace("/", "_")

    suffix = _digest_suffix(input_str)
    return sanitized[: max_length - len(suffix)] + suffix


def generate_destination(run_id: str, attribute_name: str, filename: str) -> str:
    """
    Generate a path under which a file should be saved in the storage.

    The path is generated in the format:
        <run_id>/<attribute_name>/<filename>

    The path is guaranteed not to exceed the max length. If necessary, path
    components are truncated to fit their maximum allowed lengths.

    Truncated run_id and attribute path components have a hash digest appended.
    Truncated filenames: file.ext -> file-<digest>.ext, with ext being cut it too long,
    without appending the hash digest.
    """

    run_id = _trim_and_sanitize_with_digest_suffix(run_id, MAX_RUN_ID_COMPONENT_LENGTH)
    attribute_name = _trim_with_optional_digest_suffix(attribute_name, MAX_ATTRIBUTE_PATH_COMPONENT_LENGTH)

    # Truncate the filename if necessary, keeping extension (truncated) if present.
    if len(filename) > MAX_FILENAME_PATH_COMPONENT_LENGTH:
        path = pathlib.Path(filename)
        extension = path.suffix  # includes the dot (.txt)
        filename_no_ext = path.name[: -len(extension)] if extension else path.name
        filename_no_ext = _trim_with_optional_digest_suffix(filename_no_ext, MAX_FILENAME_PATH_COMPONENT_LENGTH)
        filename = filename_no_ext + extension[:MAX_FILENAME_EXTENSION_LENGTH]

    return f"{run_id}/{attribute_name}/{filename}"
