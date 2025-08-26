from pathlib import Path

import azure.core.exceptions
from azure.core.pipeline.transport import AsyncioRequestsTransport
from azure.storage.blob.aio import BlobClient

from neptune_scale.exceptions import NeptuneFileUploadTemporaryError
from neptune_scale.util import get_logger

__all__ = ["upload_to_azure"]

logger = get_logger()


async def upload_to_azure(local_path: str, mime_type: str, storage_url: str, chunk_size: int = 4 * 1024 * 1024) -> None:
    logger.debug(f"Starting upload to Azure: {local_path}, {mime_type=}, {chunk_size=}")

    try:
        size_bytes = Path(local_path).stat().st_size
        with open(local_path, "rb") as file:
            client = BlobClient.from_blob_url(
                storage_url,
                max_block_size=chunk_size,
                max_initial_backoff=5,
                increment_base=3,
                retry_total=5,
                transport=AsyncioRequestsTransport(),
            )
            async with client:
                # Upload with default concurrency settings
                await client.upload_blob(
                    file,
                    content_type=mime_type,
                    overwrite=True,
                    length=size_bytes,
                )
    except azure.core.exceptions.AzureError as e:
        logger.debug(f"Azure SDK error, will retry uploading file {local_path}: {e}")
        raise NeptuneFileUploadTemporaryError() from e
    except Exception as e:
        logger.debug(f"Failed to upload file {local_path}: {e}")
        raise e

    logger.debug(f"Finished upload to Azure: {local_path}")
