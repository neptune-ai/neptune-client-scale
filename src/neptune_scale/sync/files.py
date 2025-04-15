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
        logger.warning(f"Error determining mime type for the provided buffer, defaulting to {DEFAULT_MIME_TYPE}: {e}")
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


_DISALLOWED_CHARS_REPLACEMENT = str.maketrans({char: "_" for char in "/."})


def _sanitize_and_trim(input_str: str, max_length: int, force_suffix: bool) -> str:
    sanitized = input_str.translate(_DISALLOWED_CHARS_REPLACEMENT)

    suffix = _digest_suffix(input_str) if force_suffix or len(sanitized) > max_length else ""
    return sanitized[: max_length - len(suffix)] + suffix


def generate_destination(run_id: str, attribute_name: str, filename: str) -> str:
    """
    Generate a path under which a file should be saved in the storage.

    The path is generated in the format:
        <run_id>/<attribute_name>/<filename>

    The path is guaranteed not to exceed the max length. If necessary, path
    components are truncated to fit their maximum allowed lengths.

    All path components have "." and "/" replaced with "_". Run id an attribute path
    always have a hash digest appended.

    Filenames have "." and "/" characters replaced with "_", except for "." in the extension.
    The extension is truncated to a maximum length of 18 characters, and
    the file stem, if necessary, is truncated with digest appended.
    """

    run_id = _sanitize_and_trim(run_id, MAX_RUN_ID_COMPONENT_LENGTH, force_suffix=True)
    attribute_name = _sanitize_and_trim(attribute_name, MAX_ATTRIBUTE_PATH_COMPONENT_LENGTH, force_suffix=True)

    # Sanitize the filename. Truncate if necessary, keeping extension (truncated) if present.
    path = pathlib.Path(filename)
    extension = path.suffix  # includes the dot (.txt)
    filename_no_ext = path.name[: -len(extension)] if extension else path.name
    filename_no_ext = _sanitize_and_trim(filename_no_ext, MAX_FILENAME_PATH_COMPONENT_LENGTH, force_suffix=False)
    filename = filename_no_ext + extension[:MAX_FILENAME_EXTENSION_LENGTH]

    return f"{run_id}/{attribute_name}/{filename}"
