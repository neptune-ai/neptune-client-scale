#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import annotations

__all__ = ("ApiClient", "with_api_errors_handling")

import functools
import os
from collections.abc import (
    Callable,
    Iterable,
    Mapping,
)
from dataclasses import dataclass
from json import JSONDecodeError
from typing import (
    Any,
    Literal,
)

import httpx
from google.protobuf.message import DecodeError as ProtobufDecodeError
from httpx import Timeout
from neptune_api import (
    AuthenticatedClient,
    Client,
)
from neptune_api.api.backend import get_client_config
from neptune_api.api.ingestion import (
    bulk_check_status,
    ingest,
)
from neptune_api.api.storage import (
    complete_multipart_upload,
    signed_url_generic,
)
from neptune_api.auth_helpers import exchange_api_key
from neptune_api.credentials import Credentials
from neptune_api.errors import (
    ApiKeyRejectedError,
    InvalidApiTokenException,
    UnableToDeserializeApiKeyError,
    UnableToExchangeApiKeyError,
    UnableToParseResponse,
    UnableToRefreshTokenError,
)
from neptune_api.models import (
    ClientConfig,
    CompleteMultipartUploadRequest,
    CreateSignedUrlsRequest,
    CreateSignedUrlsResponse,
    FileToSign,
    MultipartPart,
    Permission,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.client_pb2 import (
    RequestId,
    RequestIdList,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation
from neptune_api.types import File as BinaryContent
from neptune_api.types import Response

from neptune_scale.exceptions import (
    NeptuneConnectionLostError,
    NeptuneInvalidCredentialsError,
    NeptuneUnableToAuthenticateError,
    NeptuneUnexpectedResponseError,
)
from neptune_scale.sync.parameters import HTTP_CLIENT_NETWORKING_TIMEOUT
from neptune_scale.util.envs import ALLOW_SELF_SIGNED_CERTIFICATE
from neptune_scale.util.logger import get_logger

logger = get_logger()


@dataclass
class TokenRefreshingURLs:
    authorization_endpoint: str
    token_endpoint: str

    @classmethod
    def from_dict(cls, data: dict) -> TokenRefreshingURLs:
        return TokenRefreshingURLs(
            authorization_endpoint=data["authorization_endpoint"], token_endpoint=data["token_endpoint"]
        )


@dataclass
class FileSignRequest:
    path: str
    size: int
    permission: Literal["read", "write"]


def get_config_and_token_urls(
    *,
    credentials: Credentials,
    verify_ssl: bool,
) -> tuple[ClientConfig, TokenRefreshingURLs]:
    with Client(
        base_url=credentials.base_url,
        follow_redirects=True,
        verify_ssl=verify_ssl,
        timeout=Timeout(timeout=HTTP_CLIENT_NETWORKING_TIMEOUT),
        headers={"User-Agent": _generate_user_agent()},
    ) as client:
        try:
            config_response = get_client_config.sync_detailed(client=client)
            if config_response.status_code != 200:
                raise NeptuneUnexpectedResponseError()

            config = config_response.parsed
            urls_response = client.get_httpx_client().get(config.security.open_id_discovery)
            if not urls_response.is_success:
                raise NeptuneUnexpectedResponseError()

            token_urls = TokenRefreshingURLs.from_dict(urls_response.json())

            return config, token_urls
        except JSONDecodeError:
            raise NeptuneUnexpectedResponseError()


def create_auth_api_client(
    *,
    credentials: Credentials,
    config: ClientConfig,
    token_refreshing_urls: TokenRefreshingURLs,
    verify_ssl: bool,
) -> AuthenticatedClient:
    return AuthenticatedClient(
        base_url=credentials.base_url,
        credentials=credentials,
        client_id=config.security.client_id,
        token_refreshing_endpoint=token_refreshing_urls.token_endpoint,
        api_key_exchange_callback=exchange_api_key,
        follow_redirects=True,
        verify_ssl=verify_ssl,
        timeout=Timeout(timeout=HTTP_CLIENT_NETWORKING_TIMEOUT),
        headers={"User-Agent": _generate_user_agent()},
    )


_ILLEGAL_CHARS = str.maketrans({c: "_" for c in " ();/"})


def _generate_user_agent() -> str:
    import platform
    from importlib.metadata import version

    def sanitize(value: Callable[[], str]) -> str:
        try:
            result = value()
            return result.translate(_ILLEGAL_CHARS)
        except Exception:
            return "unknown"

    package_name = "neptune-scale"
    package_version = sanitize(lambda: version(package_name))
    additional_metadata = {
        "neptune-api": sanitize(lambda: version("neptune-api")),
        "python": sanitize(platform.python_version),
        "os": sanitize(platform.system),
    }

    additional_metadata_str = "; ".join(f"{k}={v}" for k, v in additional_metadata.items())
    return f"{package_name}/{package_version} ({additional_metadata_str})"


class ApiClient:
    def __init__(self, api_token: str) -> None:
        credentials = Credentials.from_api_key(api_key=api_token)

        verify_ssl: bool = os.environ.get(ALLOW_SELF_SIGNED_CERTIFICATE, "False").lower() in ("false", "0")

        logger.debug("Trying to connect to Neptune API")
        config, token_urls = get_config_and_token_urls(credentials=credentials, verify_ssl=verify_ssl)
        self.backend = create_auth_api_client(
            credentials=credentials,
            config=config,
            token_refreshing_urls=token_urls,
            verify_ssl=verify_ssl,
        )
        logger.debug("Connected to Neptune API")

    def submit(self, operation: RunOperation, family: str) -> Response[BinaryContent]:
        return ingest.sync_detailed(client=self.backend, body=BinaryContent(payload=operation.SerializeToString()))

    def check_batch(self, request_ids: list[str], project: str) -> Response[BinaryContent]:
        return bulk_check_status.sync_detailed(
            client=self.backend,
            project_identifier=project,
            body=BinaryContent(
                payload=RequestIdList(
                    ids=[RequestId(value=request_id) for request_id in request_ids]
                ).SerializeToString()
            ),
        )

    def close(self) -> None:
        logger.debug("Closing API client")
        self.backend.__exit__()

    def fetch_file_storage_urls(
        self,
        file_sign_requests: Iterable[FileSignRequest],
        project: str,
    ) -> Response[CreateSignedUrlsResponse]:
        files_to_sign = [
            FileToSign(
                path=request.path,
                project_identifier=project,
                permission=Permission(request.permission),
                size=request.size,
            )
            for request in file_sign_requests
        ]
        return signed_url_generic.sync_detailed(
            client=self.backend,
            body=CreateSignedUrlsRequest(
                files=files_to_sign,
            ),
        )

    def complete_multipart_upload(
        self, upload_id: str, project: str, path: str, etags: Mapping[int, str]
    ) -> Response[Any]:
        return complete_multipart_upload.sync_detailed(
            client=self.backend,
            body=CompleteMultipartUploadRequest(
                upload_id=upload_id,
                project_identifier=project,
                path=path,
                parts=[
                    MultipartPart(
                        part_number=ix,
                        etag=etag,
                    )
                    for ix, etag in etags.items()
                ],
            ),
        )


def with_api_errors_handling(func: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except (InvalidApiTokenException, UnableToDeserializeApiKeyError, ApiKeyRejectedError) as e:
            raise NeptuneInvalidCredentialsError() from e
        except (UnableToRefreshTokenError, UnableToExchangeApiKeyError) as e:
            # The errors above are raised by neptune-api when API token retrieval/refresh fails for
            # reasons other than the token being explicitly rejected by the server: network errors,
            # HTTP status != 200 etc. We should retry on these.
            raise NeptuneUnableToAuthenticateError() from e
        except httpx.RequestError as e:
            raise NeptuneConnectionLostError() from e
        except (JSONDecodeError, ProtobufDecodeError, UnableToParseResponse) as e:
            raise NeptuneUnexpectedResponseError() from e
        except Exception as e:
            raise e

    return wrapper
