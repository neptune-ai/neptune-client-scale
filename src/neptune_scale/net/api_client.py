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

__all__ = ("HostedApiClient", "MockedApiClient", "ApiClient", "backend_factory", "with_api_errors_handling")

import abc
import functools
import os
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from http import HTTPStatus
from typing import (
    Any,
    Literal,
)

import httpx
from httpx import Timeout
from neptune_api import (
    AuthenticatedClient,
    Client,
)
from neptune_api.api.backend import get_client_config
from neptune_api.api.data_ingestion import (
    check_request_status_bulk,
    submit_operation,
)
from neptune_api.auth_helpers import exchange_api_key
from neptune_api.credentials import Credentials
from neptune_api.errors import (
    InvalidApiTokenException,
    UnableToDeserializeApiKeyError,
    UnableToExchangeApiKeyError,
    UnableToRefreshTokenError,
    UnexpectedStatus,
)
from neptune_api.models import (
    ClientConfig,
    Error,
)
from neptune_api.proto.google_rpc.code_pb2 import Code
from neptune_api.proto.neptune_pb.ingest.v1.ingest_pb2 import IngestCode
from neptune_api.proto.neptune_pb.ingest.v1.pub.client_pb2 import (
    BulkRequestStatus,
    RequestId,
    RequestIdList,
    SubmitResponse,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation
from neptune_api.proto.neptune_pb.ingest.v1.pub.request_status_pb2 import RequestStatus
from neptune_api.types import Response

from neptune_scale.exceptions import (
    NeptuneConnectionLostError,
    NeptuneInvalidCredentialsError,
    NeptuneUnableToAuthenticateError,
)
from neptune_scale.sync.parameters import REQUEST_TIMEOUT
from neptune_scale.util.abstract import Resource
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


def get_config_and_token_urls(
    *, credentials: Credentials, verify_ssl: bool
) -> tuple[ClientConfig, TokenRefreshingURLs]:
    with Client(
        base_url=credentials.base_url,
        follow_redirects=True,
        verify_ssl=verify_ssl,
        timeout=Timeout(timeout=REQUEST_TIMEOUT),
    ) as client:
        config = get_client_config.sync(client=client)
        if config is None or isinstance(config, Error):
            raise RuntimeError(f"Failed to get client config: {config}")
        response = client.get_httpx_client().get(config.security.open_id_discovery)
        token_urls = TokenRefreshingURLs.from_dict(response.json())
    return config, token_urls


def create_auth_api_client(
    *, credentials: Credentials, config: ClientConfig, token_refreshing_urls: TokenRefreshingURLs, verify_ssl: bool
) -> AuthenticatedClient:
    return AuthenticatedClient(
        base_url=credentials.base_url,
        credentials=credentials,
        client_id=config.security.client_id,
        token_refreshing_endpoint=token_refreshing_urls.token_endpoint,
        api_key_exchange_callback=exchange_api_key,
        follow_redirects=True,
        verify_ssl=verify_ssl,
        timeout=Timeout(timeout=REQUEST_TIMEOUT),
    )


class ApiClient(Resource, abc.ABC):
    @abc.abstractmethod
    def submit(self, operation: RunOperation, family: str) -> Response[SubmitResponse]: ...

    @abc.abstractmethod
    def check_batch(self, request_ids: list[str], project: str) -> Response[BulkRequestStatus]: ...

    @abc.abstractmethod
    def fetch_file_storage_info(self, project: str, file_path: str, permission: Literal["read", "write"]) -> str: ...


class HostedApiClient(ApiClient):
    def __init__(self, api_token: str) -> None:
        credentials = Credentials.from_api_key(api_key=api_token)

        verify_ssl: bool = os.environ.get(ALLOW_SELF_SIGNED_CERTIFICATE, "False").lower() in ("false", "0")

        logger.debug("Trying to connect to Neptune API")
        config, token_urls = get_config_and_token_urls(credentials=credentials, verify_ssl=verify_ssl)
        self.backend = create_auth_api_client(
            credentials=credentials, config=config, token_refreshing_urls=token_urls, verify_ssl=verify_ssl
        )
        logger.debug("Connected to Neptune API")

    def submit(self, operation: RunOperation, family: str) -> Response[SubmitResponse]:
        return submit_operation.sync_detailed(client=self.backend, body=operation, family=family)

    def check_batch(self, request_ids: list[str], project: str) -> Response[BulkRequestStatus]:
        return check_request_status_bulk.sync_detailed(
            client=self.backend,
            project_identifier=project,
            body=RequestIdList(ids=[RequestId(value=request_id) for request_id in request_ids]),
        )

    def fetch_file_storage_info(self, project: str, file_path: str, permission: Literal["read", "write"]) -> str:
        return f"https://DUMMY.localhost/{project}/{file_path}"

    def close(self) -> None:
        logger.debug("Closing API client")
        self.backend.__exit__()


class MockedApiClient(ApiClient):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def submit(self, operation: RunOperation, family: str) -> Response[SubmitResponse]:
        operation_count = len(operation.update_batch.snapshots) if operation.update_batch.snapshots else 1
        request_ids = [str(uuid.uuid4()) for _ in range(operation_count)]
        response = SubmitResponse(request_id=request_ids[-1], request_ids=request_ids)
        return Response(content=b"", parsed=response, status_code=HTTPStatus.OK, headers={})

    def check_batch(self, request_ids: list[str], project: str) -> Response[BulkRequestStatus]:
        response_body = BulkRequestStatus(
            statuses=list(
                map(
                    lambda _: RequestStatus(
                        code_by_count=[RequestStatus.CodeByCount(count=1, code=Code.OK, detail=IngestCode.OK)]
                    ),
                    request_ids,
                )
            )
        )
        return Response(content=b"", parsed=response_body, status_code=HTTPStatus.OK, headers={})

    def fetch_file_storage_info(self, project: str, file_path: str, permission: Literal["read", "write"]) -> str:
        # TODO: request the actual endpoint
        return f"https://localhost:65530/{project}/{file_path}"


def backend_factory(api_token: str, mode: Literal["async", "disabled"]) -> ApiClient:
    if mode == "disabled":
        return MockedApiClient()
    return HostedApiClient(api_token=api_token)


def with_api_errors_handling(func: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except (InvalidApiTokenException, UnableToDeserializeApiKeyError):
            raise NeptuneInvalidCredentialsError()
        except (UnableToRefreshTokenError, UnableToExchangeApiKeyError, UnexpectedStatus):
            raise NeptuneUnableToAuthenticateError()
        except (httpx.ConnectError, httpx.TimeoutException, httpx.RemoteProtocolError):
            raise NeptuneConnectionLostError()
        except Exception as e:
            raise e

    return wrapper
