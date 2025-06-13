from pathlib import Path

import aiofiles
import backoff
import httpx
from httpx import AsyncClient

from neptune_scale.exceptions import NeptuneFileUploadTemporaryError
from neptune_scale.sync.parameters import (
    HTTP_CLIENT_NETWORKING_TIMEOUT,
    HTTP_REQUEST_MAX_TIME_SECONDS,
)
from neptune_scale.util import get_logger

__all__ = ["upload_to_gcp"]

logger = get_logger()


async def upload_to_gcp(file_path: str, content_type: str, signed_url: str, chunk_size: int = 16 * 1024 * 1024) -> None:
    """
    Upload a file to Google Cloud Storage using a signed URL. The upload is done in chunks, and resumed in
    case of a failure of a specific chunk upload.

    Raises NeptuneFileUploadTemporaryError if a retryable error happens, otherwise any other non-retryable exception
    that occurs.

    chunk_size must be a multiple of 256 KiB (256 * 1024 bytes) (GCS requirement)
    """

    logger.debug(f"Starting upload to GCS: {file_path}, {content_type=}, {chunk_size=}")

    try:
        async with AsyncClient(timeout=httpx.Timeout(timeout=HTTP_CLIENT_NETWORKING_TIMEOUT)) as client:
            session_uri = await _fetch_session_uri(client, signed_url, content_type)

            file_size = Path(file_path).stat().st_size
            if file_size == 0:
                await _upload_empty_file(client, session_uri)
                return

            await _upload_file(client, session_uri, file_path, file_size, chunk_size)
    except httpx.RequestError as e:
        logger.debug(f"Temporary error while uploading {file_path}: {e}")
        raise NeptuneFileUploadTemporaryError() from e
    except httpx.HTTPStatusError as e:
        logger.debug(f"HTTP {e.response.status_code} error while uploading {file_path}: {e}, {e.response.content=!r}")
        if _is_retryable_httpx_error(e):
            raise NeptuneFileUploadTemporaryError() from e
        else:
            raise

    logger.debug(f"Finished upload to GCS: {file_path}")


async def _upload_file(client: AsyncClient, session_uri: str, file_path: str, file_size: int, chunk_size: int) -> None:
    file_position = 0

    async with aiofiles.open(file_path, "rb") as file:
        while file_position < file_size:
            chunk = await file.read(chunk_size)
            if not chunk:
                raise Exception("File truncated during upload")

            upload_position = await _upload_chunk(client, session_uri, chunk, file_position, file_size)
            file_position += len(chunk)

            # If the server confirmed less that we uploaded, we need to track back to the reported position.
            if file_position != upload_position:
                logger.debug(
                    f"Server returned a different upload position: {file_position=}, {upload_position=}. "
                    f"Resuming from {upload_position}."
                )
                await file.seek(upload_position)
                file_position = upload_position

            logger.debug(f"{file_position}/{file_size} bytes uploaded.")


def _is_retryable_httpx_error(exc: Exception) -> bool:
    """Used to determine if an error is retryable. Retryable errors are:
    - All network-related errors
    - HTTP 5xx errors
    - HTTP 429 Too Many Requests
    - HTTP 400 Bad Request which covers errors related to signed URLs, see:
         https://cloud.google.com/storage/docs/xml-api/reference-status#400%E2%80%94bad
    """
    if isinstance(exc, httpx.RequestError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code
        return status_code in (400, 429) or status_code // 100 == 5

    return False


@backoff.on_predicate(backoff.expo, _is_retryable_httpx_error, max_time=HTTP_REQUEST_MAX_TIME_SECONDS)
async def _fetch_session_uri(client: AsyncClient, signed_url: str, content_type: str) -> str:
    """
    Use the signed url provided by Neptune API to start a resumable upload session.
    The actual data upload will use the returned session URI.

    See https://cloud.google.com/storage/docs/performing-resumable-uploads#chunked-upload
    """
    headers = {
        "X-goog-resumable": "start",
        # Google docs say that X-Upload-Content-Type should be provided to specify the content type of the file,
        # but it does not work. Setting Content-Type header does work.
        "Content-Type": content_type,
    }

    response = await client.post(signed_url, headers=headers)
    response.raise_for_status()

    session_uri = response.headers.get("Location")
    if session_uri is None:
        raise ValueError("Session URI not found in response headers")

    return str(session_uri)


@backoff.on_predicate(backoff.expo, _is_retryable_httpx_error, max_time=HTTP_REQUEST_MAX_TIME_SECONDS)
async def _upload_chunk(client: AsyncClient, session_uri: str, chunk: bytes, start: int, file_size: int) -> int:
    """Upload a chunk of data indicating the start-end position and total size. Returns the total number of bytes
    already uploaded to the server in a given session URI.

    Note that the returned value could be smaller than the number of bytes uploaded so far, so we always need
    to use the returned position to determine start position of the next chunk.
    """

    end = start + len(chunk) - 1  # -1 because Content-Range represents an inclusive range
    headers = {
        "Content-Length": str(len(chunk)),
        "Content-Range": f"bytes {start}-{end}/{file_size}",
    }

    response = await client.put(session_uri, headers=headers, content=chunk)

    # 308 -> chunk was saved: https://cloud.google.com/storage/docs/json_api/v1/status-codes#308_Resume_Incomplete
    if response.status_code == 308:
        range_header = response.headers.get("Range")
        # Nothing uploaded yet
        if range_header is None:
            return 0
        elif range_header.startswith("bytes=0-"):
            # Range header is 'bytes=0-LAST_BYTE_UPLOADED'. LAST_BYTE_UPLOADED is inclusive, so we need to add 1.
            return int(range_header.split("-")[1]) + 1
        else:
            raise ValueError(f"Unexpected Range header format received from server: `{range_header}`")
    # 2xx -> the upload is complete
    elif response.status_code // 100 == 2:
        return file_size

    response.raise_for_status()
    return -1  # keep mypy happy, the above line will always raise because status code is not 2xx or 308


@backoff.on_exception(backoff.expo, httpx.RequestError, max_time=HTTP_REQUEST_MAX_TIME_SECONDS)
async def _upload_empty_file(client: AsyncClient, session_uri: str) -> None:
    headers = {
        "Content-Range": "bytes */0",
        "Content-Length": "0",
    }
    response = await client.put(session_uri, headers=headers)
    response.raise_for_status()
