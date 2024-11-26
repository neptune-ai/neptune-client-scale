import os
from enum import Enum
from json import JSONDecodeError
from typing import (
    Any,
    Optional,
    Tuple,
)

import httpx

from neptune_scale.exceptions import (
    NeptuneApiTokenNotProvided,
    NeptuneBadRequestError,
    NeptuneProjectAlreadyExists,
)
from neptune_scale.net.api_client import (
    HostedApiClient,
    with_api_errors_handling,
)
from neptune_scale.util.envs import API_TOKEN_ENV_NAME

PROJECTS_PATH_BASE = "/api/backend/v1/projects"


class ProjectVisibility(Enum):
    PRIVATE = "priv"
    PUBLIC = "pub"
    WORKSPACE = "workspace"


@with_api_errors_handling
def create_project(
    name: str,
    *,
    workspace: Optional[str] = None,
    visibility: ProjectVisibility = ProjectVisibility.PRIVATE,
    description: Optional[str] = None,
    key: Optional[str] = None,
    api_token: Optional[str] = None,
) -> Tuple[str, str]:
    """
    Return a tuple of (workspace name, project name)
    """

    api_token = api_token or os.environ.get(API_TOKEN_ENV_NAME)
    if api_token is None:
        raise NeptuneApiTokenNotProvided()

    client = HostedApiClient(api_token=api_token)
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
            raise NeptuneProjectAlreadyExists()
        elif code // 100 == 4:
            raise NeptuneBadRequestError(status_code=code, reason=json.get("message"))
        raise e

    return json["organizationName"], json["name"]


def _safe_json(response: httpx.Response) -> Any:
    try:
        return response.json()
    except JSONDecodeError:
        return {}
