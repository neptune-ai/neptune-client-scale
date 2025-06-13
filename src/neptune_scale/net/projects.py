import os
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
    NeptuneApiTokenNotProvided,
    NeptuneBadRequestError,
    NeptuneProjectAlreadyExists,
)
from neptune_scale.net.api_client import (
    ApiClient,
    with_api_errors_handling,
)
from neptune_scale.util.envs import API_TOKEN_ENV_NAME

PROJECTS_PATH_BASE = "/api/backend/v1/projects"


class ProjectVisibility(Enum):
    PRIVATE = "priv"
    PUBLIC = "pub"
    WORKSPACE = "workspace"


ORGANIZATION_NOT_FOUND_RE = re.compile(r"Organization .* not found")


def _get_api_token(api_token: Optional[str]) -> str:
    api_token = api_token or os.environ.get(API_TOKEN_ENV_NAME)
    if api_token is None:
        raise NeptuneApiTokenNotProvided()

    return api_token


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
    api_token = _get_api_token(api_token)

    client = ApiClient(api_token=api_token)
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
    client = ApiClient(api_token=_get_api_token(api_token))

    params = {
        "userRelation": "viewerOrHigher",
        "sortBy": "lastViewed",
    }

    response = client.backend.get_httpx_client().request("get", PROJECTS_PATH_BASE, params=params)
    return cast(list[dict], response.json()["entries"])
