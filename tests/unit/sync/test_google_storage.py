import io
import tempfile
from unittest.mock import (
    ANY,
    AsyncMock,
    Mock,
    call,
    patch,
)

import httpx
import pytest

from neptune_scale.exceptions import NeptuneFileUploadTemporaryError
from neptune_scale.sync.google_storage import (
    _fetch_session_uri,
    _is_retryable_httpx_error,
    _upload_chunk,
    upload_to_gcp,
)


@pytest.fixture
def mock_file():
    """Yields a mock for files opened using `with aiofiles.open(...) as foo:`"""

    with patch("aiofiles.open") as mock_open, patch("neptune_scale.sync.google_storage.Path.stat") as mock_path_stat:
        mock_file = AsyncMock(wraps=io.BytesIO(b"ABCDEFGH"))
        mock_path_stat.return_value.st_size = 8
        # We need to mock the context manager for open
        mock_open.return_value.__aenter__.return_value = mock_file

        yield mock_file


@pytest.fixture
def mock_fetch_session_uri():
    with patch("neptune_scale.sync.google_storage._fetch_session_uri") as mock:
        mock.return_value = "http://localhost/session_uri"
        yield mock


@pytest.fixture
def mock_upload_chunk():
    with patch("neptune_scale.sync.google_storage._upload_chunk") as mock:
        yield mock


@pytest.fixture
def mock_upload_file():
    with patch("neptune_scale.sync.google_storage._upload_file") as mock:
        yield mock


async def test_500_fetch_session_uri_errors_are_temporary(mock_file, mock_fetch_session_uri, mock_upload_file):
    mock_fetch_session_uri.side_effect = [
        httpx.HTTPStatusError("Internal Server Error", request=AsyncMock(), response=AsyncMock(status_code=500)),
    ]

    with pytest.raises(NeptuneFileUploadTemporaryError):
        await upload_to_gcp("dummy_path", "text/plain", "http://localhost/signed_url")

    mock_fetch_session_uri.assert_called_once()
    mock_upload_file.assert_not_called()


@patch("neptune_scale.sync.google_storage._upload_empty_file")
async def test_upload_empty_file_called(mock_upload_empty_file, mock_upload_chunk, mock_fetch_session_uri):
    with tempfile.NamedTemporaryFile() as temp_file:
        await upload_to_gcp(temp_file.name, "text/plain", "http://localhost/signed_url")

    mock_fetch_session_uri.assert_called_once()
    mock_upload_empty_file.assert_called_once()

    mock_upload_chunk.assert_not_called()


async def test_upload_seeks_on_chunk_upload_status_different_than_expected(
    mock_file,
    mock_fetch_session_uri,
    mock_upload_chunk,
):
    """Test that upload seeks the file to the correct position returned by _upload_chunk,
    if upload progress reported by the server is different from the expected."""

    # We're uploading in chunks of 2 bytes, so the response of 5 is unexpected (should be 6)
    # upload_to_gcp() should seek back to position 5 and resume from there.
    mock_upload_chunk.side_effect = [2, 4, 5, 7, 8]

    await upload_to_gcp("dummy_path", "text/plain", "url", chunk_size=2)

    # See if we indeed seek the file and send correct chunks.
    mock_file.seek.assert_called_once_with(5)
    assert mock_file.read.call_count == 5

    expected_calls = [
        call(ANY, ANY, b"AB", 0, 8),
        call(ANY, ANY, b"CD", 2, 8),
        call(ANY, ANY, b"EF", 4, 8),
        call(ANY, ANY, b"FG", 5, 8),  # Partial chunk reupload happens here
        call(ANY, ANY, b"H", 7, 8),
    ]

    assert mock_upload_chunk.call_args_list == expected_calls


@pytest.mark.parametrize(
    "range_header, expected_position",
    (
        (None, 0),
        ("bytes=0-0", 1),
        ("bytes=0-99", 100),
    ),
)
async def test_upload_chunk_returns_correct_position(range_header, expected_position, mock_fetch_session_uri):
    client = AsyncMock()
    client.put.return_value = Mock(status_code=308, headers={"Range": range_header} if range_header else {})
    position = await _upload_chunk(client, "session-uri", b"test", 0, 100)
    assert position == expected_position


async def test_fetch_session_uri_reads_location_header():
    client = AsyncMock()
    client.post.return_value = Mock(status_code=200, headers={"Location": "http://session.url"})
    uri = await _fetch_session_uri(client, "signed_url", "text/plain")

    assert uri == "http://session.url"


@pytest.mark.parametrize(
    "error, is_retryable",
    (
        (httpx.RequestError("Connection error"), True),
        (httpx.HTTPStatusError("Internal server error", request=Mock(), response=Mock(status_code=500)), True),
        (httpx.HTTPError("Base HTTP Error"), False),
        (httpx.HTTPStatusError("Moved temporarily", request=Mock(), response=Mock(status_code=302)), False),
        (httpx.HTTPStatusError("Bad Request", request=Mock(), response=Mock(status_code=400)), False),
        (Exception(), False),
    ),
)
def test_is_retryable_httpx_error(error, is_retryable):
    assert _is_retryable_httpx_error(error) == is_retryable
