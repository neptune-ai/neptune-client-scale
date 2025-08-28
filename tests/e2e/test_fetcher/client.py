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

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import (
    Final,
    Optional,
)

import httpx
from neptune_api import (
    AuthenticatedClient,
    Client,
)
from neptune_api.api.backend import get_client_config
from neptune_api.auth_helpers import exchange_api_key
from neptune_api.credentials import Credentials
from neptune_api.models import ClientConfig

from neptune_scale.util.envs import (
    VERIFY_SSL,
    get_bool,
)

NEPTUNE_HTTP_REQUEST_TIMEOUT_SECONDS: Final[float] = float(os.environ.get("NEPTUNE_HTTP_REQUEST_TIMEOUT_SECONDS", "60"))
NEPTUNE_VERIFY_SSL: Final[bool] = get_bool(VERIFY_SSL, default_missing=True, default_invalid=True)


@dataclass
class TokenRefreshingURLs:
    authorization_endpoint: str
    token_endpoint: str

    @classmethod
    def from_dict(cls, data: dict) -> TokenRefreshingURLs:
        return TokenRefreshingURLs(
            authorization_endpoint=data["authorization_endpoint"], token_endpoint=data["token_endpoint"]
        )


def create_client(api_token) -> AuthenticatedClient:
    credentials = Credentials.from_api_key(api_key=api_token)
    config, token_urls = _get_config_and_token_urls(credentials=credentials, proxies=None)
    client = _create_auth_api_client(
        credentials=credentials, config=config, token_refreshing_urls=token_urls, proxies=None
    )

    return client


def _get_config_and_token_urls(
    *, credentials: Credentials, proxies: Optional[dict[str, str]]
) -> tuple[ClientConfig, TokenRefreshingURLs]:
    timeout = httpx.Timeout(NEPTUNE_HTTP_REQUEST_TIMEOUT_SECONDS)
    with Client(
        base_url=credentials.base_url, httpx_args={"mounts": proxies}, timeout=timeout, verify_ssl=NEPTUNE_VERIFY_SSL
    ) as client:
        config_response = get_client_config.sync_detailed(client=client)
        config = config_response.parsed

        urls_response = client.get_httpx_client().get(config.security.open_id_discovery)
        token_urls = TokenRefreshingURLs.from_dict(urls_response.json())

        return config, token_urls


def _create_auth_api_client(
    *,
    credentials: Credentials,
    config: ClientConfig,
    token_refreshing_urls: TokenRefreshingURLs,
    proxies: Optional[dict[str, str]],
) -> AuthenticatedClient:
    return AuthenticatedClient(
        base_url=credentials.base_url,
        credentials=credentials,
        client_id=config.security.client_id,
        token_refreshing_endpoint=token_refreshing_urls.token_endpoint,
        api_key_exchange_callback=exchange_api_key,
        httpx_args={"mounts": proxies, "http2": False},
        timeout=httpx.Timeout(NEPTUNE_HTTP_REQUEST_TIMEOUT_SECONDS),
        verify_ssl=NEPTUNE_VERIFY_SSL,
    )
