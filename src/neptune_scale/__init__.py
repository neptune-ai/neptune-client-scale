"""
Python package
"""

from __future__ import annotations

__all__ = ["Run"]

from contextlib import AbstractContextManager
from types import TracebackType

from neptune_scale.core.validation import (
    verify_max_length,
    verify_non_empty,
    verify_project_qualified_name,
    verify_type,
)
from neptune_scale.parameters import (
    MAX_FAMILY_LENGTH,
    MAX_RUN_ID_LENGTH,
)


class Run(AbstractContextManager):
    """
    Representation of tracked metadata.
    """

    Args:
        project (str): Name of the project where the run should go, in the form `workspace-name/project_name`.
        api_token (str): User's API token.
        family (str): It must be common for all runs in a given run hierarchy. Select any string.
        run_id (str): It must be unique in the project.
    """

    def __init__(self, *, project: str, api_token: str, family: str, run_id: str) -> None:
        """
        Initializes a run that logs the model-building metadata to Neptune.

        Args:
            project: Name of the project where the metadata is logged, in the form `workspace-name/project-name`.
            api_token: Your Neptune API token.
            family: Identifies related runs. For example, the same value must apply to all runs within a run hierarchy. 
                Max length: 128 characters.
            run_id: Unique identifier of a run. Must be unique within the project. Max length: 128 characters.
        """
        verify_type("api_token", api_token, str)
        verify_type("family", family, str)
        verify_type("run_id", run_id, str)

        verify_non_empty("api_token", api_token)
        verify_non_empty("family", family)
        verify_non_empty("run_id", run_id)

        verify_project_qualified_name("project", project)

        verify_max_length("family", family, MAX_FAMILY_LENGTH)
        verify_max_length("run_id", run_id, MAX_RUN_ID_LENGTH)

        self._project: str = project
        self._api_token: str = api_token
        self._family: str = family
        self._run_id: str = run_id

    def __enter__(self) -> Run:
        return self

    def close(self) -> None:
        """
        Close then run and stop tracking.
        """
        pass

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()
