"""
Python package
"""

from __future__ import annotations

__all__ = ["Run"]

import threading
from contextlib import AbstractContextManager
from datetime import datetime
from typing import Callable

from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import ForkPoint
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import Run as CreateRun
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale.api.api_client import ApiClient
from neptune_scale.core.components.abstract import (
    Resource,
    WithResources,
)
from neptune_scale.core.components.operations_queue import OperationsQueue
from neptune_scale.core.message_builder import MessageBuilder
from neptune_scale.core.proto_utils import (
    datetime_to_proto,
    make_step,
)
from neptune_scale.core.validation import (
    verify_collection_type,
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
        resume: bool = False,
        as_experiment: str | None = None,
        creation_time: datetime | None = None,
        from_run_id: str | None = None,
        from_step: int | float | None = None,
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
            resume: Whether to resume an existing run.
            as_experiment: ID of the experiment to be associated with the run.
            creation_time: Time when the run was created.
            from_run_id: ID if the Run to fork from.
            from_step: Step number to fork from.
            max_queue_size: Maximum number of operations in a queue.
            max_queue_size_exceeded_callback: Callback function triggered when a queue is full.
                Accepts two arguments:
                - Maximum size of the queue.
                - Exception that made the queue full.
        """
        verify_type("api_token", api_token, str)
        verify_type("family", family, str)
        verify_type("run_id", run_id, str)
        verify_type("resume", resume, bool)
        verify_type("as_experiment", as_experiment, (str, type(None)))
        verify_type("creation_time", creation_time, (datetime, type(None)))
        verify_type("from_run_id", from_run_id, (str, type(None)))
        verify_type("from_step", from_step, (int, float, type(None)))
        verify_type("max_queue_size", max_queue_size, int)
        verify_type("max_queue_size_exceeded_callback", max_queue_size_exceeded_callback, (Callable, type(None)))

        if resume and creation_time is not None:
            raise ValueError("`resume` and `creation_time` cannot be used together.")
        if resume and as_experiment is not None:
            raise ValueError("`resume` and `as_experiment` cannot be used together.")
        if (from_run_id is not None and from_step is None) or (from_run_id is None and from_step is not None):
            raise ValueError("`from_run_id` and `from_step` must be used together.")
        if resume and from_run_id is not None:
            raise ValueError("`resume` and `from_run_id` cannot be used together.")
        if resume and from_step is not None:
            raise ValueError("`resume` and `from_step` cannot be used together.")

        verify_non_empty("api_token", api_token)
        verify_non_empty("family", family)
        verify_non_empty("run_id", run_id)
        if as_experiment is not None:
            verify_non_empty("as_experiment", as_experiment)
        if from_run_id is not None:
            verify_non_empty("from_run_id", from_run_id)

        verify_project_qualified_name("project", project)

        verify_max_length("family", family, MAX_FAMILY_LENGTH)
        verify_max_length("run_id", run_id, MAX_RUN_ID_LENGTH)

        self._project: str = project
        self._family: str = family
        self._run_id: str = run_id

        self._lock = threading.RLock()
        self._operations_queue: OperationsQueue = OperationsQueue(
            lock=self._lock, max_size=max_queue_size, max_size_exceeded_callback=max_queue_size_exceeded_callback
        )
        self._backend: ApiClient = ApiClient(api_token=api_token)

        if not resume:
            self._create_run(
                creation_time=datetime.now() if creation_time is None else creation_time,
                as_experiment=as_experiment,
                from_run_id=from_run_id,
                from_step=from_step,
            )

    def __enter__(self) -> Run:
        return self

    @property
    def resources(self) -> tuple[Resource, ...]:
        return self._operations_queue, self._backend

    def close(self) -> None:
        """
        Stops the connection to Neptune and synchronizes all data.
        """
        super().close()

    def _create_run(
        self,
        creation_time: datetime,
        as_experiment: str | None,
        from_run_id: str | None,
        from_step: int | float | None,
    ) -> None:
        fork_point: ForkPoint | None = None
        if from_run_id is not None and from_step is not None:
            fork_point = ForkPoint(
                parent_project=self._project, parent_run_id=from_run_id, step=make_step(number=from_step)
            )

        operation = RunOperation(
            project=self._project,
            run_id=self._run_id,
            create=CreateRun(
                family=self._family,
                fork_point=fork_point,
                experiment_id=as_experiment,
                creation_time=None if creation_time is None else datetime_to_proto(creation_time),
            ),
        )
        self._backend.submit(operation=operation)
        # TODO: Enqueue on the operations queue
        # self._operations_queue.enqueue(operation=operation)

    def log(
        self,
        step: float | int | None = None,
        timestamp: datetime | None = None,
        fields: dict[str, float | bool | int | str | datetime | list | set] | None = None,
        metrics: dict[str, float] | None = None,
        add_tags: dict[str, list[str] | set[str]] | None = None,
        remove_tags: dict[str, list[str] | set[str]] | None = None,
    ) -> None:
        """
        Logs the specified metadata to Neptune.

        Args:
            step: Index of the log entry, must be increasing. If None, the highest of the already logged indexes is used.
            timestamp: Time of logging the metadata.
            fields: Dictionary of fields to log.
            metrics: Dictionary of metrics to log.
            add_tags: Dictionary of tags to add to the run.
            remove_tags: Dictionary of tags to remove from the run.

        Examples:
            ```
            >>> with Run(...) as run:
            ...     run.log(step=1, fields={"parameters/learning_rate": 0.001})
            ...     run.log(step=2, add_tags={"sys/group_tags": ["group1", "group2"]})
            ...     run.log(step=3, metrics={"metrics/loss": 0.1})
            ```

        """
        verify_type("step", step, (float, int, type(None)))
        verify_type("timestamp", timestamp, (datetime, type(None)))
        verify_type("fields", fields, (dict, type(None)))
        verify_type("metrics", metrics, (dict, type(None)))
        verify_type("add_tags", add_tags, (dict, type(None)))
        verify_type("remove_tags", remove_tags, (dict, type(None)))

        timestamp = datetime.now() if timestamp is None else timestamp
        fields = {} if fields is None else fields
        metrics = {} if metrics is None else metrics
        add_tags = {} if add_tags is None else add_tags
        remove_tags = {} if remove_tags is None else remove_tags

        verify_collection_type("`fields` keys", list(fields.keys()), str)
        verify_collection_type("`metrics` keys", list(metrics.keys()), str)
        verify_collection_type("`add_tags` keys", list(add_tags.keys()), str)
        verify_collection_type("`remove_tags` keys", list(remove_tags.keys()), str)

        verify_collection_type("`fields` values", list(fields.values()), (float, bool, int, str, datetime, list, set))
        verify_collection_type("`metrics` values", list(metrics.values()), float)
        verify_collection_type("`add_tags` values", list(add_tags.values()), (list, set))
        verify_collection_type("`remove_tags` values", list(remove_tags.values()), (list, set))

        for operation in MessageBuilder(
            project=self._project,
            run_id=self._run_id,
            step=step,
            timestamp=timestamp,
            fields=fields,
            metrics=metrics,
            add_tags=add_tags,
            remove_tags=remove_tags,
        ):
            self._backend.submit(operation=operation)
            # TODO: Enqueue on the operations queue
            # self._operations_queue.enqueue(operation=operation)
