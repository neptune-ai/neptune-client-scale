import gzip
import pathlib
import tempfile
from unittest.mock import patch

import filetype
import pytest

from neptune_scale.sync.files import (
    guess_mime_type_from_bytes,
    guess_mime_type_from_file,
)


@pytest.fixture
def mock_guess_mime():
    with patch("filetype.guess_mime", wraps=filetype.guess_mime) as mock:
        yield mock


@pytest.mark.parametrize(
    "filename, expected_mime_type, should_fall_back_to_filetype",
    (
        ("test.txt", "text/plain", False),
        ("test.unknown", "application/octet-stream", True),
    ),
)
def test_guess_mime_type_from_bytes(mock_guess_mime, filename, expected_mime_type, should_fall_back_to_filetype):
    data = b""
    mime = guess_mime_type_from_bytes(data, filename)

    assert mime == expected_mime_type

    if should_fall_back_to_filetype:
        mock_guess_mime.assert_called_once_with(data)
    else:
        mock_guess_mime.assert_not_called()


@pytest.mark.parametrize("filename, expected_mime_type", (("test.txt", "text/plain"), ("test.jpg", "image/jpeg")))
def test_guess_mime_type_from_file_no_disk_access_on_known_extension(mock_guess_mime, filename, expected_mime_type):
    mime = guess_mime_type_from_file(filename)
    assert mime == expected_mime_type
    mock_guess_mime.assert_not_called()


def test_guess_mime_type_from_file_read_disk_on_unknown_extension(mock_guess_mime):
    """Verify that guess_mime_type_from_file actually reads the file from disk
    when file extension is not recognized."""

    with tempfile.TemporaryDirectory() as temp_dir:
        filename = pathlib.Path(temp_dir) / "test.file"
        with gzip.open(filename, "wb") as f:
            f.write(b"test")

        mime = guess_mime_type_from_file(filename)
        assert mime == "application/gzip"
        mock_guess_mime.assert_called_once_with(filename)
