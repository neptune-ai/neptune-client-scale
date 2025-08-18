__all__ = ["RepositoryInfo", "read_repository_info"]

import pathlib
from dataclasses import dataclass
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Optional,
)

from neptune_scale.util.logger import get_logger
from neptune_scale.vendor.lib_programname import (
    empty_path,
    get_path_executed_script,
)

if TYPE_CHECKING:
    import git

logger = get_logger()


@dataclass
class RepositoryInfo:
    commit_id: str
    commit_message: str
    commit_author_name: str
    commit_author_email: str
    commit_date: datetime
    dirty: bool
    branch: Optional[str]
    remotes: dict[str, str]
    run_command: Optional[str]
    entry_point_path: Optional[pathlib.Path]
    head_diff_content: Optional[bytes]
    upstream_diff_commit_id: Optional[str]
    upstream_diff_content: Optional[bytes]


def read_repository_info(
    path: Optional[pathlib.Path], run_command: bool, entry_point: bool, head_diff: bool, upstream_diff: bool
) -> Optional[RepositoryInfo]:
    git_repo = _get_git_repo(path)
    if git_repo is None:
        return None

    head_commit = git_repo.head.commit
    is_dirty = git_repo.is_dirty(index=False)
    active_branch = _get_active_branch(git_repo)
    remotes = {remote.name: remote.url for remote in git_repo.remotes}

    run_command_content = None
    if run_command:
        run_command_content = _get_run_command()

    entry_point_path = None
    if entry_point:
        entry_point_path = _get_entrypoint_path()

    head_diff_content = None
    if head_diff:
        head_diff_content = _read_diff(git_repo, git_repo.head.name)

    upstream_sha = None
    upstream_diff_content = None
    if upstream_diff:
        upstream_commit = _get_relevant_upstream_commit(git_repo)
        if upstream_commit:
            upstream_sha = upstream_commit.hexsha
            upstream_diff_content = _read_diff(git_repo, upstream_sha)

    return RepositoryInfo(
        commit_id=head_commit.hexsha,
        commit_message=head_commit.message,
        commit_author_name=head_commit.author.name,
        commit_author_email=head_commit.author.email,
        commit_date=head_commit.committed_datetime,
        dirty=is_dirty,
        branch=active_branch,
        remotes=remotes,
        entry_point_path=entry_point_path,
        head_diff_content=head_diff_content,
        upstream_diff_commit_id=upstream_sha,
        upstream_diff_content=upstream_diff_content,
        run_command=run_command_content,
    )


def _get_git_repo(path: Optional[pathlib.Path]) -> Optional["git.Repo"]:
    if path is not None:
        path = path.resolve()

    try:
        import git

        try:
            return git.Repo(path, search_parent_directories=True)
        except (git.exc.NoSuchPathError, git.exc.InvalidGitRepositoryError):
            return None
    except ImportError:
        logger.warning("GitPython could not be initialized")
        return None


def _get_active_branch(repo: "git.Repo") -> str:
    active_branch = ""
    try:
        active_branch = repo.active_branch.name
    except TypeError as e:
        if str(e.args[0]).startswith("HEAD is a detached symbolic reference as it points to"):
            active_branch = "Detached HEAD"
    return active_branch


def _read_diff(repo: "git.Repo", commit_ref: str) -> Optional[bytes]:
    try:
        from git.exc import GitCommandError

        try:
            diff = repo.git.diff(commit_ref, index=False, no_ext_diff=True)

            if not isinstance(diff, str):
                return None

            # add a newline at the end (required to be a valid `patch` file)
            if diff and diff[-1] != "\n":
                diff += "\n"

            return diff.encode("utf-8")
        except GitCommandError:
            return None
    except ImportError:
        return None


def _get_relevant_upstream_commit(repo: "git.Repo") -> Optional["git.Commit"]:
    try:
        tracking_branch = repo.active_branch.tracking_branch()
    except (TypeError, ValueError):
        return None

    if tracking_branch:
        return tracking_branch.commit

    return _search_for_most_recent_ancestor(repo)


def _search_for_most_recent_ancestor(repo: "git.Repo") -> Optional["git.Commit"]:
    most_recent_ancestor: Optional[git.Commit] = None

    try:
        from git.exc import GitCommandError

        try:
            for branch in repo.heads:
                tracking_branch = branch.tracking_branch()
                if tracking_branch:
                    for ancestor in repo.merge_base(repo.head, tracking_branch.commit):
                        if not most_recent_ancestor or repo.is_ancestor(most_recent_ancestor, ancestor):
                            most_recent_ancestor = ancestor
        except GitCommandError:
            pass
    except ImportError:
        return None

    return most_recent_ancestor


def _get_entrypoint_path() -> Optional[pathlib.Path]:
    if _is_ipython():
        return None

    entrypoint_path = get_path_executed_script()
    if not isinstance(entrypoint_path, pathlib.Path):
        return None
    if entrypoint_path == empty_path:
        return None
    if not entrypoint_path.is_file():
        return None

    return entrypoint_path


def _is_ipython() -> bool:
    try:
        import IPython

        ipython = IPython.core.getipython.get_ipython()
        return ipython is not None
    except ImportError:
        return False


def _get_run_command() -> str:
    import psutil

    args = psutil.Process().cmdline()
    return " ".join(args)
