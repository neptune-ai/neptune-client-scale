import hashlib
import mimetypes
import pathlib
from typing import (
    Optional,
    Union,
    cast,
)

import filetype

from neptune_scale.sync.parameters import MAX_FILE_TARGET_PATH_LENGTH
from neptune_scale.util import get_logger

__all__ = (
    "guess_mime_type_from_file",
    "guess_mime_type_from_bytes",
    "generate_target_path",
)

DEFAULT_MIME_TYPE = "application/octet-stream"
logger = get_logger()


def guess_mime_type_from_file(local_path: Union[pathlib.Path, str], target_path: Optional[str] = None) -> Optional[str]:
    """Guess mime type by local file path and the target path. In case of an error, return None.

    We return None instead of DEFAULT_MIME_TYPE under the assumption that an error here also means
    that the file is inaccessible or not found, thus the upload will fail.
    """
    try:
        if mime := mimetypes.guess_type(local_path)[0]:
            return mime

        if target_path:
            if mime := mimetypes.guess_type(target_path)[0]:
                return mime

        if mime := filetype.guess_mime(local_path):
            return cast(str, mime)

        return DEFAULT_MIME_TYPE
    except Exception as e:
        logger.warning(f"Error determining mime type for {local_path}: {e}")
        return None


def guess_mime_type_from_bytes(data: bytes, target_path: Optional[str] = None) -> str:
    """Guess mime type by providing a buffer and the target path"""
    try:
        if target_path:
            if mime := mimetypes.guess_type(target_path)[0]:
                return mime

        if mime := filetype.guess_mime(data):
            return cast(str, mime)

        return DEFAULT_MIME_TYPE
    except Exception as e:
        logger.warning(f"Error determining mime type for the provided buffer, defaulting to ${DEFAULT_MIME_TYPE}: {e}")
        return DEFAULT_MIME_TYPE


def _ensure_length(string: str, max_length: int) -> str:
    """Ensure the string is within the specified length limit. If it's longer,
    truncate it and append a 9-character unique string to the end.

    The string is guaranteed not to be truncated at a "/", which is desirable for
    attribute paths. This is only guaranteed if the string does not contain runs of
    "/" characters.

    Example: "attr/path", max_length=14 can be truncated to "attr/-HASH1234" as it
    fits the limit. However, this would introduce a new undesired level in path
    hierarchy. Thus, it will be truncated to "attr-HASH1234".

    The string must be a valid utf-8 string, and max_length cannot be less than 11:

        - 9 bytes for "-HASH1234"
        - byte for the original name part
        - 1 byte in case we need to skip a trailing "/"
    """

    assert max_length >= 11

    if len(string) > max_length:
        digest = hashlib.blake2b(string.encode("utf-8"), digest_size=4).hexdigest()
        truncate_at = max_length - len(digest) - 1  # -1 for "-"
        if string[truncate_at - 1] == "/":
            truncate_at -= 1
        return string[:truncate_at] + "-" + digest
    return string


# If the resulting target path needs truncating, the path component will
# remain at least this many characters long
RESERVE_FILENAME_LENGTH = 64


def generate_target_path(
    run_id: str, attribute_name: str, filename: str, max_length: int = MAX_FILE_TARGET_PATH_LENGTH
) -> str:
    """
    Generate a path for storage. The path is generated in the format:
        run-id/attribute/path/file.txt

    The path is guaranteed not to exceed the max length. If necessary, path
    components are truncated to fit the length:
     - attribute name takes precedence over the filename
     - the filename is truncated to fit the remaining space, but no less
       than 64 characters, including extension
     - file extension is not truncated

    Truncated components have a hash digest appended to the end, with the format
    of "ORIG_PATH_PREFIX-[0-9a-f]{8}".
    """

    filename_reserve = min(RESERVE_FILENAME_LENGTH, len(filename))

    # Assume we want at least 32 chars for the attribute name
    if len(run_id) + 2 + filename_reserve + 32 > max_length:
        raise ValueError("The provided max_length is to small to fit the arguments")

    if "/" in filename:
        raise ValueError("Filename cannot contain '/' character")

    run_id = run_id.replace("/", "_")

    # Truncate the attribute name leaving space for the filename and 2 "/" characters
    max_attribute_length = max_length - len(run_id) - filename_reserve - 2
    assert max_attribute_length >= 32

    attribute_name = _ensure_length(attribute_name, max_attribute_length)

    # Truncate the filename if it is too long, keeping the extension as is.
    max_filename_length = max_length - len(run_id) - len(attribute_name) - 2
    if len(filename) > max_filename_length:
        filename_parts = filename.rsplit(".", maxsplit=1)
        if len(filename_parts) == 2:
            name, ext = filename_parts
        else:
            name, ext = filename, ""

        name = _ensure_length(name, max_filename_length - len(ext) - 1)
        filename = name + "." + ext if ext else name

    return f"{run_id}/{attribute_name}/{filename}"
