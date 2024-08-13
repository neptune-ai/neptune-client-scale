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

__all__ = ["ApiClient"]


from dataclasses import dataclass

from neptune_api import (
    AuthenticatedClient,
    Client,
)
from neptune_api.api.backend import get_client_config
from neptune_api.api.data_ingestion import submit_operation
from neptune_api.auth_helpers import exchange_api_key
from neptune_api.credentials import Credentials
from neptune_api.models import (
    ClientConfig,
    Error,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.client_pb2 import RequestId
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation
from neptune_api.types import Response

from neptune_scale.core.components.abstract import Resource
from neptune_scale.core.logger import logger


class ApiClient(Resource):
    def __init__(self, api_token: str) -> None:
        credentials = Credentials.from_api_key(api_key=api_token)

        logger.debug("Trying to connect to Neptune API")
        config, token_urls = get_config_and_token_urls(credentials=credentials)
        self._backend = create_auth_api_client(credentials=credentials, config=config, token_refreshing_urls=token_urls)
        logger.debug("Connected to Neptune API")

    def submit(self, operation: RunOperation, family: str) -> Response[RequestId]:
        return submit_operation.sync_detailed(client=self._backend, body=operation, family=family)

    def close(self) -> None:
        logger.debug("Closing API client")
        self._backend.__exit__()


@dataclass
class TokenRefreshingURLs:
    authorization_endpoint: str
    token_endpoint: str

    @classmethod
    def from_dict(cls, data: dict) -> TokenRefreshingURLs:
        return TokenRefreshingURLs(
            authorization_endpoint=data["authorization_endpoint"], token_endpoint=data["token_endpoint"]
        )


def get_config_and_token_urls(*, credentials: Credentials) -> tuple[ClientConfig, TokenRefreshingURLs]:
    with Client(base_url=credentials.base_url) as client:
        config = get_client_config.sync(client=client)
        if config is None or isinstance(config, Error):
            raise RuntimeError(f"Failed to get client config: {config}")
        response = client.get_httpx_client().get(config.security.open_id_discovery)
        token_urls = TokenRefreshingURLs.from_dict(response.json())
    return config, token_urls


def create_auth_api_client(
    *, credentials: Credentials, config: ClientConfig, token_refreshing_urls: TokenRefreshingURLs
) -> AuthenticatedClient:
    return AuthenticatedClient(
        base_url=credentials.base_url,
        credentials=credentials,
        client_id=config.security.client_id,
        token_refreshing_endpoint=token_refreshing_urls.token_endpoint,
        api_key_exchange_callback=exchange_api_key,
    )
