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

__all__ = ["upload_to_s3"]

logger = get_logger()


async def upload_to_s3(file_path: str, content_type: str, signed_url: str) -> None:
    """
    Upload a file to S3 using a signed URL. The upload is done in a single part and resumed in case of a failure.
    TODO: Implement multipart upload for larger files.

    Raises NeptuneFileUploadTemporaryError if a retryable error happens, otherwise any other non-retryable exception
    that occurs.
    """

    logger.debug(f"Starting upload to S3: {file_path}, {content_type=}")

    try:
        async with AsyncClient(timeout=httpx.Timeout(timeout=HTTP_CLIENT_NETWORKING_TIMEOUT)) as client:
            await _upload_file(client, signed_url, file_path, content_type)
    except httpx.RequestError as e:
        logger.debug(f"Temporary error while uploading {file_path}: {e}")
        raise NeptuneFileUploadTemporaryError() from e
    except httpx.HTTPStatusError as e:
        logger.debug(f"HTTP {e.response.status_code} error while uploading {file_path}: {e}, {e.response.content=!r}")
        if _is_retryable_httpx_error(e):
            raise NeptuneFileUploadTemporaryError() from e
        else:
            raise

    logger.debug(f"Finished upload to S3: {file_path}")


async def _upload_file(client: AsyncClient, signed_url: str, file_path: str, content_type: str) -> None:
    # TODO: Implement multipart upload for larger files
    file_size = Path(file_path).stat().st_size
    if file_size == 0:
        await _upload_part(client, signed_url, b"", content_type)
        return

    async with aiofiles.open(file_path, "rb") as file:
        content = await file.read()
        if not content:
            raise Exception("File truncated during upload")

        await _upload_part(client, signed_url, content, content_type)


def _is_retryable_httpx_error(exc: Exception) -> bool:
    """Used to determine if an error is retryable. The standard retry policy is as defined in:
    https://github.com/boto/botocore/blob/master/botocore/retries/standard.py
    or https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html
    It might be worth considering to use the same policy for S3 uploads, but for now we retry on:
    - All network-related errors
    - HTTP 5xx errors
    - HTTP 429 Too Many Requests
    """
    if isinstance(exc, httpx.RequestError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code
        return status_code in (429,) or status_code // 100 == 5

    return False


@backoff.on_predicate(backoff.expo, _is_retryable_httpx_error, max_time=HTTP_REQUEST_MAX_TIME_SECONDS)
async def _upload_part(client: AsyncClient, session_uri: str, content: bytes, content_type: str) -> None:
    # The docs at https://docs.aws.amazon.com/AmazonS3/latest/userguide/PresignedUrlUploadObject.html
    # provide Content-Type as the only header
    headers = {"Content-Type": content_type}

    response = await client.put(session_uri, headers=headers, content=content)
    response.raise_for_status()
