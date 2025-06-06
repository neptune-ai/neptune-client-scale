import os
import tempfile
from unittest.mock import (
    AsyncMock,
    MagicMock,
    patch,
)

import httpx
import pytest

from neptune_scale.exceptions import NeptuneFileUploadTemporaryError
from neptune_scale.sync.google_storage import upload_to_gcp


@pytest.fixture
def tmp_file_name():
    # For Windows, we need to close the file before yielding, as the OS won't allow reading a while
    # that is already open. This is why we remove it manually.
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        try:
            tmp.write(b"test")
            tmp.flush()
            tmp.close()
            yield tmp.name
        finally:
            os.remove(tmp.name)


@pytest.fixture
def mock_sleep():
    with patch("asyncio.sleep", new_callable=AsyncMock) as mock:
        yield mock


@pytest.fixture
def mock_async_client_cls():
    with patch("neptune_scale.sync.google_storage.AsyncClient") as mock:
        yield mock


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


@pytest.fixture
def mock_upload_empty_file():
    with patch("neptune_scale.sync.google_storage._upload_empty_file") as mock:
        yield mock


@pytest.fixture
def mock_query_resume_position():
    with patch("neptune_scale.sync.google_storage._query_resume_position") as mock:
        yield mock


async def test_500_fetch_session_uri_errors_are_temporary(tmp_file_name, mock_fetch_session_uri, mock_upload_file):
    mock_fetch_session_uri.side_effect = [
        httpx.HTTPStatusError("Internal Server Error", request=AsyncMock(), response=AsyncMock(status_code=500)),
        httpx.Response(status_code=200, headers={"Location": "http://localhost/session_uri"}),
    ]

    with pytest.raises(NeptuneFileUploadTemporaryError):
        await upload_to_gcp(tmp_file_name, "text/plain", "http://localhost/signed_url")

    mock_fetch_session_uri.assert_called_once()
    mock_upload_file.assert_not_called()


@pytest.mark.parametrize(
    "error",
    (
        httpx.HTTPStatusError("Bad Request", request=AsyncMock(), response=AsyncMock(status_code=400)),
        httpx.HTTPStatusError("Unauthorized", request=AsyncMock(), response=AsyncMock(status_code=401)),
        httpx.HTTPStatusError("Bad Request", request=AsyncMock(), response=AsyncMock(status_code=300)),
    ),
)
async def test_upload_chunk_no_retries_on_fatal_http_errors(
    error, tmp_file_name, mock_sleep, mock_fetch_session_uri, mock_upload_chunk, mock_query_resume_position
):
    mock_upload_chunk.side_effect = error
    mock_query_resume_position.return_value = 0

    with pytest.raises(type(error)):
        await upload_to_gcp(tmp_file_name, "text/plain", "http://localhost/signed_url")

    mock_upload_chunk.assert_called_once()
    mock_query_resume_position.assert_not_called()


@pytest.mark.parametrize(
    "side_effect",
    (
        [None, None],
        [
            None,
            httpx.HTTPStatusError("Internal Server Error", request=AsyncMock(), response=AsyncMock(status_code=500)),
            None,
            None,
        ],
        [None, Exception(), None, None],
        [httpx.TransportError(""), None, None],
        [
            None,
            httpx.HTTPStatusError("Internal Server Error", request=AsyncMock(), response=AsyncMock(status_code=501)),
            None,
            httpx.HTTPStatusError("Internal Server Error", request=AsyncMock(), response=AsyncMock(status_code=503)),
            None,
            None,
        ],
    ),
)
async def test_upload_chunk_retries_on_non_fatal_errors(
    side_effect,
    tmp_file_name,
    mock_sleep,
    mock_fetch_session_uri,
    mock_upload_chunk,
    mock_query_resume_position,
):
    """Test that _upload_chunk retries on exceptions and sleeps between retries."""

    mock_upload_chunk.side_effect = side_effect
    # We always reset to position 0 on retries, so to "complete" the upload we
    # always have two successful calls to _upload_chunk at the end of side_effect
    mock_query_resume_position.return_value = 0

    await upload_to_gcp(tmp_file_name, "text/plain", "http://localhost/signed_url", 2)

    assert mock_upload_chunk.call_count == len(side_effect)
    # We should sleep for each exception raised
    assert mock_sleep.call_count == sum(1 for e in side_effect if isinstance(e, Exception))


async def test_upload_empty_file_called(
    mock_upload_empty_file, mock_upload_chunk, mock_query_resume_position, mock_fetch_session_uri
):
    with tempfile.NamedTemporaryFile() as temp_file:
        await upload_to_gcp(temp_file.name, "text/plain", "http://localhost/signed_url")

    mock_fetch_session_uri.assert_called_once()
    mock_upload_empty_file.assert_called_once()

    mock_query_resume_position.assert_not_called()
    mock_upload_chunk.assert_not_called()


@patch("neptune_scale.sync.google_storage.Path.stat")
@patch("builtins.open")
async def test_upload_seeks_on_resume(
    mock_open, mock_path_stat, mock_sleep, mock_fetch_session_uri, mock_upload_chunk, mock_query_resume_position
):
    """Test that upload seeks the file to the correct position on resume after an error in _upload_chunk."""
    mock_path_stat.return_value.st_size = 8
    mock_query_resume_position.return_value = 2

    mock_file = MagicMock()
    mock_upload_chunk.side_effect = [None, None, Exception("error"), None, None, None]
    mock_file.read.side_effect = [b"AB", b"CD", b"upload_chunk error is after this read", b"CD", b"EF", b"GH"]

    # Need to mock the context manager for open
    mock_open.return_value.__enter__.return_value = mock_file

    await upload_to_gcp("dummy_path", "text/plain", "http://localhost/signed_url", chunk_size=2)

    mock_file.seek.assert_called_once_with(2)

    mock_query_resume_position.assert_called_once()
    mock_sleep.assert_called_once()
    assert mock_upload_chunk.call_count == mock_file.read.call_count


async def test_dont_retry_when_query_resume_position_reports_upload_completed(
    mock_sleep, tmp_file_name, mock_fetch_session_uri, mock_upload_chunk, mock_query_resume_position
):
    """Test that upload does not retry when _query_resume_position reports the upload is already complete."""

    mock_upload_chunk.side_effect = Exception("upload_chunk error")
    mock_query_resume_position.return_value = 4  # Length of tmp_file contents

    await upload_to_gcp(tmp_file_name, "text/plain", "http://localhost/signed_url")

    mock_query_resume_position.assert_called_once()
    mock_sleep.assert_called_once()
    mock_upload_chunk.assert_called_once()
