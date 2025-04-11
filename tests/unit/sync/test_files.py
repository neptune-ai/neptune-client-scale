import gzip
import pathlib
import re
import tempfile
from unittest.mock import (
    Mock,
    call,
    patch,
)

import filetype
import pytest
from azure.core.exceptions import (
    AzureError,
    ClientAuthenticationError,
    HttpResponseError,
)

from neptune_scale.sync.errors_tracking import ErrorsQueue
from neptune_scale.sync.files import (
    _ensure_length,
    generate_target_path,
    guess_mime_type_from_bytes,
    guess_mime_type_from_file,
)
from neptune_scale.sync.operations_repository import (
    FileUploadRequest,
    OperationsRepository,
    SequenceId,
)
from neptune_scale.sync.sync_process import FileUploaderThread


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
    "local_path, target_path, expected_mime_type",
    (
        ("test.txt", "test.jpg", "text/plain"),
        ("test.jpg", "test.txt", "image/jpeg"),
        ("test.unknown", "test.jpg", "image/jpeg"),
    ),
)
def test_guess_mime_type_from_file_no_disk_access_on_known_extension(
    mock_guess_mime, local_path, target_path, expected_mime_type
):
    mime = guess_mime_type_from_file(local_path, target_path)
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


@pytest.fixture
def mock_backend_factory():
    with patch("neptune_scale.sync.sync_process.backend_factory") as mock:
        yield mock


@pytest.fixture
def mock_fetch_file_storage_urls():
    with patch("neptune_scale.sync.sync_process.fetch_file_storage_urls") as mock:
        mock.return_value = {"target/text.txt": "text-url", "target/image.jpg": "image-url"}
        yield mock


@pytest.fixture
def mock_upload_file():
    with patch("neptune_scale.sync.sync_process.upload_file") as mock:
        yield mock


@pytest.fixture
def mock_operations_repository():
    return Mock(spec=OperationsRepository)


@pytest.fixture
def mock_errors_queue():
    return Mock(spec=ErrorsQueue)


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield pathlib.Path(temp_dir)


@pytest.fixture
def temporary_file_upload_request(temp_dir):
    filename = str(temp_dir / "text.txt")
    data = "test"
    with open(filename, "w") as f:
        f.write(data)

    request = FileUploadRequest(filename, "target/text.txt", "text/plain", len(data), True, SequenceId(1))
    return request


@pytest.fixture
def user_file_upload_request(temp_dir):
    filename = str(temp_dir / "image.jpg")
    data = "test"
    with open(filename, "w") as f:
        f.write(data)

    request = FileUploadRequest(filename, "target/image.jpg", "image/jpeg", len(data), False, SequenceId(2))
    return request


@pytest.fixture
def uploader_thread(
    api_token,
    mock_backend_factory,
    mock_operations_repository,
    mock_fetch_file_storage_urls,
    mock_upload_file,
    mock_errors_queue,
):
    thread = FileUploaderThread(
        project="workspace/project",
        api_token=api_token,
        operations_repository=mock_operations_repository,
        errors_queue=mock_errors_queue,
    )

    yield thread

    if thread.is_alive():
        thread.interrupt()
        thread.join()


def test_file_uploader_thread_successful_upload_flow(
    temp_dir,
    uploader_thread,
    mock_fetch_file_storage_urls,
    mock_upload_file,
    mock_operations_repository,
    mock_errors_queue,
    temporary_file_upload_request,
    user_file_upload_request,
):
    """Test the happy path flow of the file uploader thread."""

    mock_operations_repository.get_file_upload_requests.side_effect = [
        [temporary_file_upload_request, user_file_upload_request],
        [],
        [],
    ]

    uploader_thread.start()
    uploader_thread.interrupt(remaining_iterations=1)
    uploader_thread.join()

    assert mock_operations_repository.get_file_upload_requests.called

    # Storage URLs should be fetched for all files
    mock_fetch_file_storage_urls.assert_called_once_with(
        uploader_thread._neptune_client, "workspace/project", ["target/text.txt", "target/image.jpg"]
    )

    expected_calls = [
        call(
            temporary_file_upload_request.source_path,
            temporary_file_upload_request.mime_type,
            temporary_file_upload_request.size_bytes,
            "text-url",
        ),
        call(
            user_file_upload_request.source_path,
            user_file_upload_request.mime_type,
            user_file_upload_request.size_bytes,
            "image-url",
        ),
    ]
    # All files should be uploaded
    mock_upload_file.assert_has_calls(expected_calls)

    # Completed files should be deleted from repository
    mock_operations_repository.delete_file_upload_requests.assert_has_calls(
        [call([temporary_file_upload_request.sequence_id]), call([user_file_upload_request.sequence_id])]
    )

    # Temporary files should be deleted, user files should be not
    assert not pathlib.Path(temporary_file_upload_request.source_path).exists()
    assert pathlib.Path(user_file_upload_request.source_path).exists()

    # No errors should be emitted
    mock_errors_queue.put.assert_not_called()


def test_file_uploader_thread_terminal_error(
    temp_dir,
    uploader_thread,
    mock_fetch_file_storage_urls,
    mock_upload_file,
    mock_operations_repository,
    mock_errors_queue,
    user_file_upload_request,
):
    """Uploader thread should not terminate on an upload error that is not an Azure error. This should
    be treated as a terminal error for the given upload, but further uploads should be processed."""

    mock_operations_repository.get_file_upload_requests.side_effect = [
        [
            FileUploadRequest("no-such-file", "target/text.txt", "text/plain", 123, False, SequenceId(11)),
            user_file_upload_request,
        ],
        [],
        [],
    ]

    mock_upload_file.side_effect = [FileNotFoundError, None]

    uploader_thread.start()
    uploader_thread.interrupt(remaining_iterations=1)
    uploader_thread.join()

    mock_fetch_file_storage_urls.assert_called_once_with(
        uploader_thread._neptune_client, "workspace/project", ["target/text.txt", "target/image.jpg"]
    )

    # An upload attempt should be made for both files
    expected_calls = [
        call(
            "no-such-file",
            "text/plain",
            123,
            "text-url",
        ),
        call(
            user_file_upload_request.source_path,
            user_file_upload_request.mime_type,
            user_file_upload_request.size_bytes,
            "image-url",
        ),
    ]
    mock_upload_file.assert_has_calls(expected_calls)

    # Both files should be deleted from the repository
    mock_operations_repository.delete_file_upload_requests.assert_has_calls(
        [call([11]), call([user_file_upload_request.sequence_id])]
    )

    assert pathlib.Path(user_file_upload_request.source_path).exists()

    assert mock_errors_queue.put.call_count == 1
    assert isinstance(mock_errors_queue.put.call_args[0][0], FileNotFoundError)


@pytest.mark.parametrize("upload_error", [AzureError(""), HttpResponseError, ClientAuthenticationError])
def test_file_uploader_thread_non_terminal_error(
    upload_error,
    temp_dir,
    uploader_thread,
    mock_fetch_file_storage_urls,
    mock_upload_file,
    mock_operations_repository,
    mock_errors_queue,
    user_file_upload_request,
):
    """Uploader thread should retry uploads on any Azure errors."""

    # We will fail 2 times before succeeding
    mock_upload_file.side_effect = [upload_error, upload_error, None]

    mock_operations_repository.get_file_upload_requests.side_effect = [
        [user_file_upload_request],
        [user_file_upload_request],
        [user_file_upload_request],
        [],
        [],
    ]

    uploader_thread.start()
    uploader_thread.interrupt(remaining_iterations=3)
    uploader_thread.join()

    # Signed urls should be requested for each attempt
    mock_fetch_file_storage_urls.assert_has_calls(
        [call(uploader_thread._neptune_client, "workspace/project", [user_file_upload_request.target_path])] * 3
    )

    # An upload attempt should be made for 2 failures and the final success
    mock_upload_file.assert_has_calls(
        [
            call(
                user_file_upload_request.source_path,
                user_file_upload_request.mime_type,
                user_file_upload_request.size_bytes,
                "image-url",
            )
        ]
        * 3
    )

    # The file request should be deleted from the repository only once
    mock_operations_repository.delete_file_upload_requests.assert_has_calls(
        [call([user_file_upload_request.sequence_id])]
    )

    mock_errors_queue.put.assert_not_called()


@pytest.mark.parametrize(
    "string, max_length, match_result",
    [
        ("A" * 20, 100, r"^A{20}$"),
        ("A" * 11, 11, "^A{11}$"),
        ("A" * 100, 19, r"^A{10}-[0-9a-f]{8}$"),
        # Make sure we don't truncate with a trailing "/" before the unique string
        ("attr/nested/path", 14, "^attr-[0-9a-f]{8}$"),
    ],
)
def test_ensure_length(string, max_length, match_result):
    result = _ensure_length(string, max_length)

    assert len(result) <= max_length
    assert re.fullmatch(match_result, result), f"{result} did not match {match_result}"


@pytest.mark.parametrize(
    "run_id, attribute_name, filename, max_length, expect_run_id, match_attribute, match_filename",
    # Note that the trailing "-[0-9a-f]{8}" regex matches the hash digest used when
    # truncating path components.
    [
        # Run_id and attribute name are short, run-id does not need sanitizing
        ("run-id", "attribute/path", "file.txt", 1024, "run-id", r"^attribute/path$", r"^file.txt$"),
        # Run_id and attribute name are short, run-id needs sanitizing
        ("run/id", "attribute/path", "file.txt", 1024, "run_id", r"^attribute/path$", r"^file.txt$"),
        # Exact match of max length with no truncation
        ("R", "A" * 100, "X", 104, "R", r"^A{100}$", r"^X$"),
        # Long attribute name should not truncate the filename if it's short enough
        (
            "run-id",
            "attribute/path/" + "A" * 100,
            "file.txt",
            128,
            "run-id",
            r"^attribute/path/A*-[0-9a-f]{8}$",
            r"^file.txt$",
        ),
        # Long file and attribute names truncate both, attribute name taking precedence
        # File extension must be kept, maximum file length is 64 characters.
        (
            "run-id",
            "attribute/" + "A" * 200,
            "file" + "X" * 100 + ".txt",
            128,
            "run-id",
            r"^attribute/A*-[0-9a-f]{8}$",
            r"^fileX{47}-[0-9a-f]{8}\.txt$",
        ),
        # Short attribute name, long file name. Attribute name should not be truncated if there is room to truncate
        # the filename.
        (
            "run-id",
            "attribute/path",
            "file" + "X" * 1000 + ".txt",
            128,
            "run-id",
            r"^attribute/path$",
            r"^fileX*-[0-9a-f]{8}\.txt$",
        ),
    ],
)
def test_generate_target_path(
    run_id, attribute_name, filename, max_length, expect_run_id, match_attribute, match_filename
):
    result = generate_target_path(run_id, attribute_name, filename, max_length)
    # +2 is for "/" separators
    if len(run_id) + len(attribute_name) + len(filename) + 2 >= max_length:
        assert len(result) == max_length, "Did not use all the available space"
    else:
        assert len(result) <= max_length

    run_id_component, tail = result.split("/", maxsplit=1)
    attribute_component, file_component = tail.rsplit("/", maxsplit=1)

    assert run_id_component == expect_run_id
    assert re.fullmatch(match_attribute, attribute_component), "Attribute component did not match"
    assert re.fullmatch(match_filename, file_component), "File component did not match"
