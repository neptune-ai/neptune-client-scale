__all__ = ["RepositoryInfo", "read_repository_info"]

import pathlib
from dataclasses import dataclass
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Optional,
)

from neptune_scale.util.logger import get_logger

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
    remotes: dict[str, str]  # TODO: sanitization of the keys?
    # "{namespace}/entry_point": file_ref  # the content of the file at __main__.__file__ -  not logged by default
    # "{namespace}/diff/head": file_ref # equivalent to git diff HEAD, so both added/stages changes and not
    # "{namespace}/diff/{commit_id on the upstream branch}": file_ref # neptune client 1.x also provides a diff from the last shared commit found on the tracking remote branch


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
        logger.warn("GitPython could not be initialized")
        return None


def _get_active_branch(repo: "git.Repo") -> str:
    active_branch = ""
    try:
        active_branch = repo.active_branch.name
    except TypeError as e:
        if str(e.args[0]).startswith("HEAD is a detached symbolic reference as it points to"):
            active_branch = "Detached HEAD"
    return active_branch


def read_repository_info(path: Optional[pathlib.Path]) -> Optional[RepositoryInfo]:
    git_repo = _get_git_repo(path)
    if git_repo is None:
        return None

    commit = git_repo.head.commit
    active_branch = _get_active_branch(git_repo)
    remotes = {remote.name: remote.url for remote in git_repo.remotes}

    return RepositoryInfo(
        commit_id=commit.hexsha,
        commit_message=commit.message,
        commit_author_name=commit.author.name,
        commit_author_email=commit.author.email,
        commit_date=commit.committed_datetime,
        dirty=git_repo.is_dirty(index=False, untracked_files=True),
        branch=active_branch,
        remotes=remotes,
    )
