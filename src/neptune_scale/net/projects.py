#
# Copyright (c) 2025, Neptune Labs Sp. z o.o.
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

import re
from enum import Enum
from json import JSONDecodeError
from typing import (
    Any,
    Optional,
    cast,
)

import httpx

from neptune_scale.exceptions import (
    NeptuneBadRequestError,
    NeptuneProjectAlreadyExists,
)
from neptune_scale.net.api_client import (
    HostedApiClient,
    with_api_errors_handling,
)
from neptune_scale.sync.util import ensure_api_token

PROJECTS_PATH_BASE = "/api/backend/v1/projects"


class ProjectVisibility(Enum):
    PRIVATE = "priv"
    PUBLIC = "pub"
    WORKSPACE = "workspace"


ORGANIZATION_NOT_FOUND_RE = re.compile(r"Organization .* not found")


@with_api_errors_handling
def create_project(
    workspace: str,
    name: str,
    *,
    visibility: ProjectVisibility = ProjectVisibility.PRIVATE,
    description: Optional[str] = None,
    key: Optional[str] = None,
    fail_if_exists: bool = False,
    api_token: Optional[str] = None,
) -> None:
    client = HostedApiClient(api_token=ensure_api_token(api_token))
    visibility = ProjectVisibility(visibility)

    body = {
        "name": name,
        "description": description,
        "projectKey": key,
        "organizationIdentifier": workspace,
        "visibility": visibility.value,
    }

    response = client.backend.get_httpx_client().request("post", PROJECTS_PATH_BASE, json=body)
    json = _safe_json(response)

    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        code = e.response.status_code
        if code == 409:
            if fail_if_exists:
                raise NeptuneProjectAlreadyExists()
        # We need to match plain text, as this is what the backend returns
        elif code == 404 and ORGANIZATION_NOT_FOUND_RE.match(response.text):
            raise NeptuneBadRequestError(status_code=code, reason=f"Workspace '{workspace}' not found")
        elif code // 100 == 4:
            raise NeptuneBadRequestError(status_code=code, reason=json.get("message"))
        else:
            raise e


def _safe_json(response: httpx.Response) -> Any:
    try:
        return response.json()
    except JSONDecodeError:
        return {}


def get_project_list(*, api_token: Optional[str] = None) -> list[dict]:
    client = HostedApiClient(api_token=ensure_api_token(api_token))

    params = {
        "userRelation": "viewerOrHigher",
        "sortBy": "lastViewed",
    }

    response = client.backend.get_httpx_client().request("get", PROJECTS_PATH_BASE, params=params)
    return cast(list[dict], response.json()["entries"])
