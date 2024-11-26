import re
from typing import (
    Optional,
    Tuple,
    cast,
)

from neptune_scale.api.validation import verify_type
from neptune_scale.net import projects
from neptune_scale.net.projects import ProjectVisibility

PROJECT_QUALIFIED_NAME_RE = re.compile(r"^((?P<workspace>[^/]+)/)?(?P<project>[^/]+)$")


def create_project(
    name: str,
    *,
    workspace: Optional[str] = None,
    visibility: str = ProjectVisibility.PRIVATE.value,
    description: Optional[str] = None,
    key: Optional[str] = None,
    api_token: Optional[str] = None,
) -> str:
    """Creates a new project in a Neptune workspace.

    Args:
        name: The name for the project in Neptune. Can contain letters and hyphens. For example, "classification".
            If you leave out the workspace argument, include the workspace name here,
            in the form "workspace-name/project-name". For example, "ml-team/classification".
        workspace: Name of your Neptune workspace.
            If None, it will be parsed from the name argument.
        visibility: Level of privacy for the project. Options:
            - "pub": Public. Anyone on the internet can see it.
            - "priv": Private. Only users specifically assigned to the project can access it. Requires a plan with
                project-level access control.
            - "workspace" (team workspaces only): Accessible to all workspace members.
            The default is "priv".
        description: Project description.
            If None, it will be left empty.
        key: Project identifier. Must contain 1-10 upper case letters or numbers (at least one letter).
            For example, "CLS2". If you leave it out, Neptune generates a project key for you.
        api_token: Account's API token.
            If None, the value of the NEPTUNE_API_TOKEN environment variable is used.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.

    Returns:
        The name of the new project created.
    """

    verify_type("name", name, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("visibility", visibility, str)
    verify_type("description", description, (str, type(None)))
    verify_type("key", key, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    workspace, name = extract_workspace_and_project(name=name, workspace=workspace)
    workspace, name = projects.create_project(
        name, workspace=workspace, visibility=visibility, description=description, key=key, api_token=api_token
    )

    return normalize_project_name(name, workspace)


def extract_workspace_and_project(name: str, workspace: Optional[str] = None) -> Tuple[str, str]:
    project_spec = PROJECT_QUALIFIED_NAME_RE.search(name)

    if not project_spec:
        raise ValueError(f"Invalid project name `{name}`")

    extracted_workspace, extracted_project_name = (
        project_spec["workspace"],
        project_spec["project"],
    )

    if not workspace and not extracted_workspace:
        raise ValueError("Workspace not provided in neither project name or the `workspace` parameter.")

    if workspace and extracted_workspace and workspace != extracted_workspace:
        raise ValueError(
            f"The provided `workspace` argument `{workspace} is different from the one in project name `{name}`"
        )

    final_workspace_name = cast(str, extracted_workspace or workspace)

    return final_workspace_name, extracted_project_name


def normalize_project_name(name: str, workspace: Optional[str] = None) -> str:
    extracted_workspace_name, extracted_project_name = extract_workspace_and_project(name=name, workspace=workspace)

    return f"{extracted_workspace_name}/{extracted_project_name}"
