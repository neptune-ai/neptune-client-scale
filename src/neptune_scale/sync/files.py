import mimetypes
import pathlib
from typing import (
    Union,
    cast,
)

import filetype

DEFAULT_MIME_TYPE = "application/octet-stream"


def guess_mime_type_from_file(path: Union[pathlib.Path, str]) -> str:
    """Guess mime type by local file path"""
    if mime := mimetypes.guess_type(path)[0]:
        return mime

    if mime := filetype.guess_mime(path):
        return cast(str, mime)

    return DEFAULT_MIME_TYPE


def guess_mime_type_from_bytes(data: bytes, target_path: str) -> str:
    """Guess mime type by providing a buffer and the target path"""
    if mime := mimetypes.guess_type(target_path)[0]:
        return mime

    if mime := filetype.guess_mime(data):
        return cast(str, mime)

    return DEFAULT_MIME_TYPE
