"""
Python package
"""

from __future__ import annotations

__all__ = ["Run"]

import os
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
from neptune_scale.core.components.errors_monitor import ErrorsMonitor
from neptune_scale.core.components.errors_queue import ErrorsQueue
from neptune_scale.core.components.operations_queue import OperationsQueue
from neptune_scale.core.metadata_splitter import MetadataSplitter
from neptune_scale.core.serialization import (
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
from neptune_scale.envs import (
    API_TOKEN_ENV_NAME,
    PROJECT_ENV_NAME,
)
from neptune_scale.parameters import (
    MAX_FAMILY_LENGTH,
    MAX_QUEUE_SIZE,
    MAX_RUN_ID_LENGTH,
)


class Run(WithResources, AbstractContextManager):
    """
    Representation of tracked metadata.

    Methods:
        close(): Synchronizes all remaining data and closes the connection to Neptune.
        log(): Logs the specified metadata to Neptune.
    """

    def __init__(
        self,
        *,
        family: str,
        run_id: str,
        project: str | None = None,
        api_token: str | None = None,
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
            family (str): Identifies related runs. All runs of the same lineage must have the same `family` value.
                Max length: 128 characters.
            run_id (str): Identifier of a run. Must be unique within the project. Max length: 128 characters.
            project (str): Name of the project where the metadata is logged, in the form `workspace-name/project-name`.
                If not provided, the value of the `NEPTUNE_PROJECT` environment variable is used.
            api_token (str): Your Neptune API token. If not provided, the value of the `NEPTUNE_API_TOKEN` environment
                variable is used.
            resume (bool): If `False` (default), creates a new run. To continue an existing run, set to `True` and pass
                the ID of an existing run to the `run_id` argument.
                To fork a run, use `from_run_id` and `from_step` instead.
            as_experiment (str): To mark a run as an experiment, pass an experiment ID.
            creation_time (datetime): Custom creation time of the run.
            from_run_id (str): To forking off an existing run, pass the ID of the run to fork from.
            from_step (int): To fork off an existing run, pass the step number to fork from.
            max_queue_size (int): Maximum number of operations allowed in the queue.
            max_queue_size_exceeded_callback (Callable[[int, BaseException], None]): Callback function triggered when
                the queue is full. The function should take two arguments:
                1. Maximum size of the queue.
                2. Exception that made the queue full.

        Examples:

            Create a new run:

            ```
            from neptune_scale import Run

            with Run(
                project="team-alpha/project-x",
                api_token="h0dHBzOi8aHR0cHM6...Y2MifQ==",
                family="aquarium",
                run_id="likable-barracuda",
            ) as run:
                ...
            ```

            Create a forked run and mark it as an experiment:

            ```
            from neptune_scale import Run

            with Run(
                family="aquarium",
                run_id="adventurous-barracuda",
                as_experiment="swim-further",
                from_run_id="likable-barracuda",
                from_step=102,
            ) as run:
                ...
            ```
        """
        verify_type("family", family, str)
        verify_type("run_id", run_id, str)
        verify_type("resume", resume, bool)
        verify_type("project", project, (str, type(None)))
        verify_type("api_token", api_token, (str, type(None)))
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

        project = project or os.environ.get(PROJECT_ENV_NAME)
        verify_non_empty("project", project)
        assert project is not None  # mypy
        input_project: str = project

        api_token = api_token or os.environ.get(API_TOKEN_ENV_NAME)
        verify_non_empty("api_token", api_token)
        assert api_token is not None  # mypy
        input_api_token: str = api_token

        verify_non_empty("family", family)
        verify_non_empty("run_id", run_id)
        if as_experiment is not None:
            verify_non_empty("as_experiment", as_experiment)
        if from_run_id is not None:
            verify_non_empty("from_run_id", from_run_id)

        verify_project_qualified_name("project", project)

        verify_max_length("family", family, MAX_FAMILY_LENGTH)
        verify_max_length("run_id", run_id, MAX_RUN_ID_LENGTH)

        self._project: str = input_project
        self._family: str = family
        self._run_id: str = run_id

        self._lock = threading.RLock()
        self._operations_queue: OperationsQueue = OperationsQueue(
            lock=self._lock, max_size=max_queue_size, max_size_exceeded_callback=max_queue_size_exceeded_callback
        )
        self._errors_queue: ErrorsQueue = ErrorsQueue()
        self._errors_monitor = ErrorsMonitor(errors_queue=self._errors_queue)
        self._backend: ApiClient = ApiClient(api_token=input_api_token)

        self._errors_monitor.start()

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
        return (
            self._operations_queue,
            self._backend,
            self._errors_monitor,
            self._errors_queue,
        )

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
        self._backend.submit(operation=operation, family=self._family)
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

        splitter: MetadataSplitter = MetadataSplitter(
            project=self._project,
            run_id=self._run_id,
            step=step,
            timestamp=timestamp,
            fields=fields,
            metrics=metrics,
            add_tags=add_tags,
            remove_tags=remove_tags,
        )

        for operation in splitter:
            self._backend.submit(operation=operation, family=self._family)
            # TODO: Enqueue on the operations queue
            # self._operations_queue.enqueue(operation=operation)
