import asyncio
import random
from pathlib import Path

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

# Maximum number of retries for chunk upload. Note that this applies only to the actual data chunks.
# Fetching upload session and querying resume position is retried separately using @backoff.
MAX_RETRIES = 6


async def upload_to_gcp(file_path: str, content_type: str, signed_url: str, chunk_size: int = 16 * 1024 * 1024) -> None:
    """
    Upload a file to Google Cloud Storage using a signed URL. The upload is done in chunks, and resumed in
    case of a failure of a specific chunk upload.

    Raises NeptuneFileUploadTemporaryError if a retryable error happens, otherwise any other non-retryable exception
    that occurs.

    chunk_size must be a multiple of 256 KiB (256 * 1024 bytes) (GCS requirement)
    """

    logger.debug(f"Starting upload to GCS: {file_path}")

    try:
        async with AsyncClient(timeout=httpx.Timeout(timeout=HTTP_CLIENT_NETWORKING_TIMEOUT)) as client:
            session_uri = await _fetch_session_uri(client, signed_url, content_type)

            file_size = Path(file_path).stat().st_size
            if file_size == 0:
                await _upload_empty_file(client, session_uri)
                return

            await _upload_file(client, session_uri, file_path, file_size, chunk_size)
    except httpx.RequestError as e:
        raise NeptuneFileUploadTemporaryError() from e
    except httpx.HTTPStatusError as e:
        # Internal server errors (5xx) are temporary
        if e.response.status_code // 100 == 5:
            raise NeptuneFileUploadTemporaryError() from e
        else:
            raise

    logger.debug(f"Finished upload to GCS: {file_path}")


async def _upload_file(client: AsyncClient, session_uri: str, file_path: str, file_size: int, chunk_size: int) -> None:
    file_position = 0
    num_retries = 0

    with open(file_path, "rb") as file:
        while file_position < file_size:
            chunk = file.read(chunk_size)
            if not chunk:
                raise Exception("File truncated during upload")

            try:
                await _upload_chunk(client, session_uri, chunk, file_position, file_size)
                file_position += len(chunk)
                num_retries = 0
                logger.debug(f"{file_position}/{file_size} bytes uploaded.")
            except Exception as e:
                logger.debug(f"Error uploading chunk: {e}")

                # HTTP status errors that are not 5xx should not be retried
                if isinstance(e, httpx.HTTPStatusError) and e.response.status_code // 100 != 5:
                    raise

                num_retries += 1
                if num_retries > MAX_RETRIES:
                    raise Exception("Max retries reached while uploading file to GCS") from e

                # Retry after exponential backoff with jitter
                sleep_time = (2**num_retries) + random.randint(0, 1000) / 1000.0
                await asyncio.sleep(sleep_time)

                file_position = await _query_resume_position(client, session_uri, file_size)
                if file_position >= file_size:
                    break

                file.seek(file_position)


def _is_retryable_httpx_error(exc: Exception) -> bool:
    """Used in @backoff.on_predicate to determine if an error is retryable. All network-related errors
    and HTTP 500 errors are considered retryable."""
    if isinstance(exc, httpx.RequestError):
        return True
    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code // 100 == 5:
        return True
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
        # Google docs say that X-Uploaded-Content-Type can be provided, but for some reason
        # it doesn't work, however Content-Type works just fine.
        "Content-Type": content_type,
        "Host": "storage.googleapis.com",
    }

    response = await client.post(signed_url, headers=headers)
    response.raise_for_status()

    session_uri = response.headers.get("Location")
    if session_uri is None:
        raise ValueError("Session URI not found in response headers")

    return str(session_uri)


async def _upload_chunk(client: AsyncClient, session_uri: str, chunk: bytes, start: int, file_size: int) -> None:
    end = start + len(chunk) - 1  # -1 because Content-Range represents an inclusive range
    headers = {
        "Content-Length": str(len(chunk)),
        "Content-Range": f"bytes {start}-{end}/{file_size}",
    }

    response = await client.put(session_uri, headers=headers, content=chunk)

    if response.status_code in (308, 200, 201):
        # 200 or 201 -> the upload is complete
        # 308 -> chunk was saved: https://cloud.google.com/storage/docs/json_api/v1/status-codes#308_Resume_Incomplete
        return

    response.raise_for_status()


@backoff.on_predicate(backoff.expo, predicate=_is_retryable_httpx_error, max_time=HTTP_REQUEST_MAX_TIME_SECONDS)
async def _query_resume_position(client: AsyncClient, session_uri: str, file_size: int) -> int:
    """
    Query Google Storage for the current upload position. If the upload is completed, return value larger
    than file_size.

    A request might've been processes by GCS correctly, but due to network issues we might not have
    received the response -- so we always query the current position after a there is a chunk upload error.
    """

    headers = {
        "Content-Range": f"bytes */{file_size}",
        "Content-Length": "0",
    }

    response = await client.put(session_uri, headers=headers)
    # 2xx - upload already completed
    if response.status_code // 100 == 2:
        return file_size + 1
    elif response.status_code == 308:
        range_header = response.headers.get("Range")
        if not range_header:
            return 0  # Nothing uploaded yet

        if range_header.startswith("bytes=0-"):
            # Range header is 'bytes=0-LAST_BYTE_UPLOADED'
            return int(range_header.split("-")[1]) + 1  # +1 to resume from the next byte
        else:
            raise ValueError(f"Unexpected Range header format received from server: {range_header}")
    else:
        response.raise_for_status()
        return -1  # keep mypy happy, the above line will always raise because status code is not 2xx


@backoff.on_exception(backoff.expo, httpx.RequestError, max_time=HTTP_REQUEST_MAX_TIME_SECONDS)
async def _upload_empty_file(client: AsyncClient, session_uri: str) -> None:
    headers = {
        "Content-Range": "bytes */0",
        "Content-Length": "0",
    }
    response = await client.put(session_uri, headers=headers)
    response.raise_for_status()
