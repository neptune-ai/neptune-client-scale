import io
import mimetypes
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Union

from neptune_scale.types import File


@dataclass(frozen=True)
class FileInfo:
    """
    Basic file information stored on the backend. We pass it to code parts that log attributes
    and perform the actual file upload.
    """

    path: str
    mime_type: str
    size_bytes: int

    @classmethod
    def from_user_file(cls, file: File, run_id: str, attribute_path: str) -> "FileInfo":
        target_path = determine_target_path(file, run_id, attribute_path)

        if isinstance(file.source, str):
            mime_type = guess_mime_type(file.source)
        else:
            mime_type = guess_mime_type(target_path)
        size_bytes = source_size(file.source)

        return cls(path=target_path, mime_type=mime_type, size_bytes=size_bytes)


def determine_target_path(
    file: File,
    run_id: str,
    attribute_path: str,
) -> str:
    # Target path always takes precedence as-is
    if file.target_path:
        return file.target_path

    if file.target_base_name:
        return "/".join((run_id, attribute_path, file.target_base_name))

    # We know that file.source is a str here because of the validation in File.__post_init__
    assert isinstance(file.source, str)
    path = Path(file.source)
    unique = str(uuid.uuid4())
    basename = f"{path.stem}-{unique}{path.suffix}"

    return "/".join((run_id, attribute_path, basename))


def guess_mime_type(path: str) -> str:
    """
    Guess the MIME type of file based on the local path, or target path, whichever is provided.
    """

    # TODO: consider using python-magic or similar for better detection
    mime_type, _ = mimetypes.guess_type(path)
    return mime_type or "application/octet-stream"


def source_size(source: Union[str, io.BytesIO]) -> int:
    if isinstance(source, str):
        return Path(source).stat().st_size

    return len(source.getbuffer())


def verify_file_readable(local_path: str) -> None:
    path = Path(local_path)
    # We let PermissionError raised by is_file() propagate
    if not path.is_file():
        raise ValueError(f"{local_path} does not exist or is not a file.")
