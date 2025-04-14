import gzip
import pathlib
import re
import tempfile
from unittest.mock import patch

import filetype
import pytest

from neptune_scale.sync.files import (
    MAX_ATTRIBUTE_PATH_COMPONENT_LENGTH,
    MAX_FILENAME_PATH_COMPONENT_LENGTH,
    MAX_RUN_ID_COMPONENT_LENGTH,
    _ensure_length,
    _sanitize_run_id,
    generate_destination,
    guess_mime_type_from_bytes,
    guess_mime_type_from_file,
)
from neptune_scale.sync.parameters import MAX_FILE_DESTINATION_LENGTH


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


@pytest.mark.parametrize(
    "local_path, destination, expected_mime_type",
    (
        ("test.txt", "test.jpg", "text/plain"),
        ("test.jpg", "test.txt", "image/jpeg"),
        ("test.unknown", "test.jpg", "image/jpeg"),
    ),
)
def test_guess_mime_type_from_file_no_disk_access_on_known_extension(
    mock_guess_mime, local_path, destination, expected_mime_type
):
    mime = guess_mime_type_from_file(local_path, destination)
    assert mime == expected_mime_type
    mock_guess_mime.assert_not_called()


def test_guess_mime_type_from_file_no_such_file():
    assert guess_mime_type_from_file("no-such-file", "unknown") is None


def test_guess_mime_type_from_file_read_disk_on_unknown_extension(mock_guess_mime):
    """Verify that guess_mime_type_from_file actually reads the file from disk
    when file extension is not recognized."""

    with tempfile.TemporaryDirectory() as temp_dir:
        filename = pathlib.Path(temp_dir) / "test.file"
        with gzip.open(filename, "wb") as f:
            f.write(b"test")

        mime = guess_mime_type_from_file(filename, "test-file")
        assert mime == "application/gzip"
        mock_guess_mime.assert_called_once_with(filename)


@pytest.mark.parametrize(
    "string, max_length, match_result",
    [
        ("A" * 20, 100, r"^A{20}$"),
        ("A" * 20, 20, r"^A{20}$"),
        ("A" * 100, 27, r"^A{10}-[0-9a-f]{16}$"),
    ],
)
def test_ensure_length(string, max_length, match_result):
    result = _ensure_length(string, max_length)

    assert len(result) <= max_length
    assert re.fullmatch(match_result, result), f"{result} did not match {match_result}"


@pytest.mark.parametrize(
    "run_id, max_length, match",
    [
        ("run-id", 100, r"^run-id$"),
        ("run/id/slashed", 100, r"^run_id_slashed-[0-9a-f]{16}$"),
        # 100 - 17 (digest) - 6 ("run_id") -> 77 A's remaining
        ("run/id" + "A" * 100, 100, r"^run_idA{77}-[0-9a-f]{16}$"),
    ],
)
def test_sanitize_run_id(run_id, max_length, match):
    result = _sanitize_run_id(run_id, max_length)
    assert len(result) <= max_length
    assert re.fullmatch(match, result), f"{result} did not match {match}"


@pytest.mark.parametrize(
    "run_id, attribute_name, filename, match_run_id, match_attribute, match_filename",
    # Note that the trailing "-[0-9a-f]{16}" regex matches the hash digest used when
    # truncating path components.
    [
        # Run_id and attribute name are short, run-id does not need sanitizing
        ("run-id", "attribute/path", "file.txt", "^run-id$", r"^attribute/path$", r"^file.txt$"),
        # Run_id and attribute name are short, run-id needs sanitizing
        ("run/id", "attribute/path", "file.txt", r"^run_id-[0-9a-f]{16}$", r"^attribute/path$", r"^file.txt$"),
        # Exact match of max length with no truncation
        ("R" * 300, "A" * 300, "X" * 198, "^R{300}$", r"^A{300}$", r"^X{198}$"),
        # Truncation of all components
        ("R" * 500, "A" * 500, "X" * 500, "^R{283}-[0-9a-f]{16}$", r"^A{283}-[0-9a-f]{16}$", r"^X{181}-[0-9a-f]{16}$"),
        # Long filename should be truncated, with extension shortened as well
        (
            "run-id",
            "attribute/path",
            "F" * 500 + "." + "E" * 100,
            "^run-id$",
            r"^attribute/path$",
            r"^F+-[0-9a-f]{16}\.E{32}$",
        ),
    ],
)
def test_generate_destination(run_id, attribute_name, filename, match_run_id, match_attribute, match_filename):
    result = generate_destination(run_id, attribute_name, filename)
    # +2 is for "/" separators
    if len(run_id) + len(attribute_name) + len(filename) + 2 >= MAX_FILE_DESTINATION_LENGTH:
        assert len(result) == MAX_FILE_DESTINATION_LENGTH, "Did not use all the available space"
    else:
        assert len(result) <= MAX_FILE_DESTINATION_LENGTH

    run_id_component, tail = result.split("/", maxsplit=1)
    attribute_component, file_component = tail.rsplit("/", maxsplit=1)

    assert len(run_id_component) <= MAX_RUN_ID_COMPONENT_LENGTH
    assert len(attribute_component) <= MAX_ATTRIBUTE_PATH_COMPONENT_LENGTH
    assert len(file_component) <= MAX_FILENAME_PATH_COMPONENT_LENGTH

    assert re.fullmatch(match_run_id, run_id_component), "RunId component did not match"
    assert re.fullmatch(match_attribute, attribute_component), "Attribute component did not match"
    assert re.fullmatch(match_filename, file_component), "File component did not match"
