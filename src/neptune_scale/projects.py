import re
from typing import (
    Optional,
    cast,
)

from neptune_scale.api.validation import verify_type
from neptune_scale.net import projects
from neptune_scale.net.projects import ProjectVisibility

PROJECT_QUALIFIED_NAME_RE = re.compile(r"^((?P<workspace>[\w\-.]+)/)?(?P<project>[\w\-.]+)$")


def create_project(
    name: str,
    *,
    workspace: Optional[str] = None,
    visibility: str = ProjectVisibility.PRIVATE.value,
    description: Optional[str] = None,
    key: Optional[str] = None,
    fail_if_exists: bool = False,
    api_token: Optional[str] = None,
) -> str:
    """Creates a new project in a Neptune workspace.

    Args:
        name (str): Name of the project. Can contain letters and hyphens (-). For example, "project-x".
        workspace (str, optional): Name of your Neptune workspace.
            You can omit this argument if you include the workspace name in the `name` argument.
        visibility: Level of privacy for the project. Options:
            - "pub": Public. Anyone on the internet can see it.
            - "priv" (default): Private. Only users specifically assigned to the project can access it. Requires a plan with
                project-level access control.
            - "workspace" (team workspaces only): Accessible to all workspace members.
        description: Project description. If None, it's left empty.
        key: Project identifier. Must contain 1-10 upper case letters or numbers (at least one letter).
            For example, "PX2". If you leave it out, Neptune generates a project key for you.
        fail_if_exists: If the project already exists and this flag is set to `True`, an error is raised.
        api_token: Account's API token.
            If not provided, the value of the NEPTUNE_API_TOKEN environment variable is used (recommended).

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
    projects.create_project(
        workspace=workspace,
        name=name,
        visibility=visibility,
        description=description,
        key=key,
        fail_if_exists=fail_if_exists,
        api_token=api_token,
    )

    return normalize_project_name(name, workspace)


def extract_workspace_and_project(name: str, workspace: Optional[str] = None) -> tuple[str, str]:
    """Return a tuple of (workspace name, project name) from the provided
    fully qualified project name, or a name + workspace

    >>> extract_workspace_and_project("my-own.workspace_/pr_oj-ect")
    ('my-own.workspace_', 'pr_oj-ect')
    >>> extract_workspace_and_project("project", "workspace")
    ('workspace', 'project')
    >>> extract_workspace_and_project("workspace/project", "workspace")
    ('workspace', 'project')
    >>> extract_workspace_and_project("workspace/project", "another_workspace")
    Traceback (most recent call last):
    ...
    ValueError: The provided `workspace` argument `another_workspace` is different ...
    >>> extract_workspace_and_project("project")
    Traceback (most recent call last):
    ...
    ValueError: Workspace not provided ...
    >>> extract_workspace_and_project("workspace/project!@#")
    Traceback (most recent call last):
    ...
    ValueError: Invalid project name ...
    """
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
            f"The provided `workspace` argument `{workspace}` is different from the one in project name `{name}`"
        )

    final_workspace_name = cast(str, extracted_workspace or workspace)

    return final_workspace_name, extracted_project_name


def normalize_project_name(name: str, workspace: Optional[str] = None) -> str:
    extracted_workspace_name, extracted_project_name = extract_workspace_and_project(name=name, workspace=workspace)

    return f"{extracted_workspace_name}/{extracted_project_name}"


def list_projects(*, api_token: Optional[str] = None) -> list[str]:
    """Lists projects that the account has access to.

    Args:
        api_token: Account's API token.
            If None, the value of the NEPTUNE_API_TOKEN environment variable is used.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.

    Returns:
        List of project names in the form "workspace-name/project-name".
    """

    verify_type("api_token", api_token, (str, type(None)))

    return [
        normalize_project_name(project["name"], project["organizationName"])
        for project in projects.get_project_list(api_token=api_token)
    ]
