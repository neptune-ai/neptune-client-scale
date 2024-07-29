"""
Python package
"""

from __future__ import annotations

__all__ = ["Run"]

import threading
from contextlib import AbstractContextManager
from typing import Callable

from neptune_scale.core.components.abstract import (
    Resource,
    WithResources,
)
from neptune_scale.core.components.operations_queue import OperationsQueue
from neptune_scale.core.validation import (
    verify_max_length,
    verify_non_empty,
    verify_project_qualified_name,
    verify_type,
)
from neptune_scale.parameters import (
    MAX_FAMILY_LENGTH,
    MAX_QUEUE_SIZE,
    MAX_RUN_ID_LENGTH,
)


class Run(WithResources, AbstractContextManager):
    """
    Representation of tracked metadata.
    """

    def __init__(
        self,
        *,
        project: str,
        api_token: str,
        family: str,
        run_id: str,
        max_queue_size: int = MAX_QUEUE_SIZE,
        max_queue_size_exceeded_callback: Callable[[int, BaseException], None] | None = None,
    ) -> None:
        """
        Initializes a run that logs the model-building metadata to Neptune.

        Args:
            project: Name of the project where the metadata is logged, in the form `workspace-name/project-name`.
            api_token: Your Neptune API token.
            family: Identifies related runs. For example, the same value must apply to all runs within a run hierarchy.
                Max length: 128 characters.
            run_id: Unique identifier of a run. Must be unique within the project. Max length: 128 characters.
            max_queue_size: Maximum number of operations that can be queued before the queue is considered full.
            max_queue_size_exceeded_callback: Callback function triggered when a queue is full.
                Accepts two arguments:
                - Maximum size of the queue.
                - Exception that made the queue full.
        """
        verify_type("api_token", api_token, str)
        verify_type("family", family, str)
        verify_type("run_id", run_id, str)
        verify_type("max_queue_size", max_queue_size, int)
        verify_type("max_queue_size_exceeded_callback", max_queue_size_exceeded_callback, (Callable, type(None)))

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

        self._lock = threading.RLock()
        self._operations_queue: OperationsQueue = OperationsQueue(
            lock=self._lock, max_size=max_queue_size, max_size_exceeded_callback=max_queue_size_exceeded_callback
        )

    def __enter__(self) -> Run:
        return self

    @property
    def resources(self) -> tuple[Resource, ...]:
        return (self._operations_queue,)

    def close(self) -> None:
        """
        Stops the connection to Neptune and synchronizes all data.
        """
        super().close()
