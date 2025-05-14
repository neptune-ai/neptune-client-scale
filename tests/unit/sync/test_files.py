import gzip
import pathlib
import re
import tempfile
from decimal import Decimal
from unittest.mock import patch

import filetype
import pytest

from neptune_scale.sync.files import (
    MAX_ATTRIBUTE_PATH_COMPONENT_LENGTH,
    MAX_FILENAME_EXTENSION_LENGTH,
    MAX_FILENAME_PATH_COMPONENT_LENGTH,
    MAX_RUN_ID_COMPONENT_LENGTH,
    MAX_SERIES_STEP_PATH_COMPONENT_LENGTH,
    NO_STEP_PLACEHOLDER,
    _sanitize_and_trim,
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
    "run_id, max_length, force_suffix, match",
    [
        ("run-id", 100, True, r"^run-id-[0-9a-f]{16}$"),
        ("run/id/slashed.dotted./", 100, True, r"^run_id_slashed_dotted__-[0-9a-f]{16}$"),
        # 100 - 17 (digest) - 6 ("run_id") -> 77 A's remaining
        ("run/id" + "A" * 100, 100, True, r"^run_idA{77}-[0-9a-f]{16}$"),
        ("A" * 20, 100, False, r"^A{20}$"),
        ("A" * 20, 20, False, r"^A{20}$"),
        ("A" * 100, 27, False, r"^A{10}-[0-9a-f]{16}$"),
    ],
)
def test_sanitize_and_trim(run_id, max_length, match, force_suffix):
    result = _sanitize_and_trim(run_id, max_length, force_suffix=force_suffix)
    assert len(result) <= max_length
    assert re.fullmatch(match, result), f"{result} did not match {match}"


@pytest.mark.parametrize(
    "run_id, attribute_name, filename, match_run_id, match_attribute, match_filename",
    # Note that the trailing "-[0-9a-f]{16}" regex matches the hash digest used when
    # truncating path components.
    [
        # Slashes in run id and attribute name should be replaced with underscores,
        # and both components should always have digest suffixes.
        # File should be preserved if there are no dots apart from the extension.
        (
            "/run/id",
            "attribute/path/",
            "file.txt",
            "^_run_id-[0-9a-f]{16}$",
            r"^attribute_path_-[0-9a-f]{16}$",
            r"^file.txt$",
        ),
        # File name should never contain dots, if there is no extension
        (
            "/run/id",
            "attribute/path/",
            "file.txt...",
            "^_run_id-[0-9a-f]{16}$",
            r"^attribute_path_-[0-9a-f]{16}$",
            r"^file_txt___$",
        ),
        # Exact match of max length taking digest into account
        (
            "R" * 283,
            "A" * 263,
            "F" * 180 + "." + "E" * 17,
            "^R{283}-[0-9a-f]{16}$",
            r"^A{263}-[0-9a-f]{16}$",
            r"^F{180}\.E{17}$",
        ),
        # Truncation of all components
        (
            "R" * 500,
            "A" * 500,
            "F" * 500 + "." + "E" * 100,
            "^R{283}-[0-9a-f]{16}$",
            r"^A{263}-[0-9a-f]{16}$",
            r"^F{163}-[" r"0-9a-f]{16}\.E{17}$",
        ),
        # Long filename should be truncated, with extension shortened as well
        (
            "run-id",
            "attribute-path",
            "F" * 500 + "." + "E" * 100,
            "^run-id-[0-9a-f]{16}$",
            r"^attribute-path-[0-9a-f]{16}$",
            r"^F+-[0-9a-f]{16}\.E{17}$",
        ),
    ],
)
def test_generate_destination(run_id, attribute_name, filename, match_run_id, match_attribute, match_filename):
    result = generate_destination(run_id, attribute_name, filename, step=None)
    if len(run_id) + len(attribute_name) + len(NO_STEP_PLACEHOLDER) + len(filename) + 3 >= MAX_FILE_DESTINATION_LENGTH:
        assert len(result) == MAX_FILE_DESTINATION_LENGTH - MAX_SERIES_STEP_PATH_COMPONENT_LENGTH + len(
            NO_STEP_PLACEHOLDER
        ), "Did not use all the available space"
    else:
        assert len(result) <= MAX_FILE_DESTINATION_LENGTH

    run_id_component, attribute_component, step_component, file_component = result.split("/")

    assert len(run_id_component) <= MAX_RUN_ID_COMPONENT_LENGTH
    assert len(attribute_component) <= MAX_ATTRIBUTE_PATH_COMPONENT_LENGTH
    assert len(step_component) <= MAX_SERIES_STEP_PATH_COMPONENT_LENGTH
    assert len(file_component) <= MAX_FILENAME_PATH_COMPONENT_LENGTH + MAX_FILENAME_EXTENSION_LENGTH

    assert re.fullmatch(match_run_id, run_id_component), "RunId component did not match"
    assert re.fullmatch(match_attribute, attribute_component), "Attribute component did not match"
    assert step_component == NO_STEP_PLACEHOLDER, f"Step component should be {NO_STEP_PLACEHOLDER}"
    assert re.fullmatch(match_filename, file_component), "File component did not match"


@pytest.mark.parametrize(
    "run_id, attribute_name, step, filename, match_run_id, match_attribute, match_step, match_filename",
    # Note that the trailing "-[0-9a-f]{16}" regex matches the hash digest used when
    # truncating path components.
    [
        (
            "/run/id",
            "attribute/path/",
            1.0,
            "file.txt",
            "^_run_id-[0-9a-f]{16}$",
            r"^attribute_path_-[0-9a-f]{16}$",
            "^000000000001_000000$",
            r"^file.txt$",
        ),
        # Exact match of max length taking digest into account
        (
            "R" * 283,
            "A" * 283,
            Decimal("999999999999.999999"),
            "F" * 180 + "." + "E" * 17,
            "^R{283}-[0-9a-f]{16}$",
            r"^A{263}-[0-9a-f]{16}$",
            "^999999999999_999999$",
            r"^F{180}\.E{17}$",
        ),
        # Truncation of all components
        (
            "R" * 500,
            "A" * 500,
            Decimal("9999999999999.999999"),
            "F" * 500 + "." + "E" * 100,
            "^R{283}-[0-9a-f]{16}$",
            r"^A{263}-[0-9a-f]{16}$",
            r"^9{2}-[0-9a-f]{16}$",
            r"^F{163}-[" r"0-9a-f]{16}\.E{17}$",
        ),
    ],
)
def test_generate_destination_series(
    run_id, attribute_name, step, filename, match_run_id, match_attribute, match_step, match_filename
):
    result = generate_destination(run_id, attribute_name, filename, step)
    # # +3 is for "/" separators
    if (
        len(run_id) + len(attribute_name) + MAX_SERIES_STEP_PATH_COMPONENT_LENGTH + len(filename) + 3
        >= MAX_FILE_DESTINATION_LENGTH
    ):
        assert len(result) == MAX_FILE_DESTINATION_LENGTH, "Did not use all the available space"
    else:
        assert len(result) <= MAX_FILE_DESTINATION_LENGTH

    run_id_component, attribute_component, step_component, file_component = result.split("/")

    assert len(run_id_component) <= MAX_RUN_ID_COMPONENT_LENGTH
    assert len(attribute_component) <= MAX_ATTRIBUTE_PATH_COMPONENT_LENGTH
    assert len(step_component) <= MAX_SERIES_STEP_PATH_COMPONENT_LENGTH
    assert len(file_component) <= MAX_FILENAME_PATH_COMPONENT_LENGTH + MAX_FILENAME_EXTENSION_LENGTH

    assert re.fullmatch(match_run_id, run_id_component), "RunId component did not match"
    assert re.fullmatch(match_attribute, attribute_component), "Attribute component did not match"
    assert re.fullmatch(match_step, step_component), "Step component did not match"
    assert re.fullmatch(match_filename, file_component), "File component did not match"
