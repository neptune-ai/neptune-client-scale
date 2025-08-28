"""
Python package
"""

from __future__ import annotations

import atexit
import base64
import binascii
import itertools
import json
import mimetypes
import multiprocessing
import os
import re
import threading
import time
import uuid
from collections.abc import (
    Callable,
    Mapping,
)
from contextlib import AbstractContextManager
from dataclasses import (
    asdict,
    dataclass,
    is_dataclass,
)
from datetime import datetime
from multiprocessing.process import BaseProcess
from pathlib import Path
from types import TracebackType
from typing import (
    Any,
    Literal,
    Optional,
    Union,
    cast,
)
from urllib.parse import quote_plus

from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import ForkPoint
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import Run as CreateRun

from neptune_scale.api.validation import (
    verify_max_length,
    verify_non_empty,
    verify_project_qualified_name,
    verify_type,
    verify_value_between,
)
from neptune_scale.exceptions import (
    NeptuneApiTokenNotProvided,
    NeptuneDatabaseConflict,
    NeptuneProjectNotProvided,
    NeptuneSynchronizationStopped,
)
from neptune_scale.logging.console_log_capture import ConsoleLogCaptureThread
from neptune_scale.sync.errors_tracking import ErrorsMonitor
from neptune_scale.sync.files import (
    generate_destination,
    guess_mime_type_from_bytes,
    guess_mime_type_from_file,
)
from neptune_scale.sync.lag_tracking import LagTracker
from neptune_scale.sync.metadata_splitter import (
    FileRefData,
    MetadataSplitter,
    Metrics,
    datetime_to_proto,
    histograms_to_update_run_snapshots,
    make_step,
    sanitize_attribute_path,
    string_series_to_update_run_snapshots,
)
from neptune_scale.sync.operations_repository import (
    FileUploadRequest,
    OperationsRepository,
)
from neptune_scale.sync.parameters import (
    MAX_EXPERIMENT_NAME_LENGTH,
    MAX_RUN_ID_LENGTH,
    OPERATION_REPOSITORY_POLL_SLEEP_TIME,
)
from neptune_scale.sync.supervisor import ProcessSupervisor
from neptune_scale.sync.sync_process import run_sync_process
from neptune_scale.types import (
    File,
    Histogram,
)
from neptune_scale.util import (
    envs,
    source_tracking,
)
from neptune_scale.util.envs import (
    API_TOKEN_ENV_NAME,
    MODE_ENV_NAME,
    PROJECT_ENV_NAME,
)
from neptune_scale.util.generate_run_id import generate_run_id
from neptune_scale.util.logger import (
    ThrottledLogger,
    get_logger,
)
from neptune_scale.util.timer import Timer

__all__ = ["Run"]

logger = get_logger()


@dataclass
class SourceTrackingConfig:
    """
    Specifies what kind of source code information to log.

    Args:
        namespace: Path of the namespace where the logged information is stored. Default is "source_code".
        repository: Path to the Git repository. If not provided, Neptune detects it by searching the working
            directory and its parent directories.
        upload_run_command: Whether to log the script execution command. Defaults to True.
        upload_entry_point: Whether to log the entry point file. Defaults to False.
        upload_diff_head: Whether to log the diff between your current working directory and the last commit.
            Defaults to False.
        upload_diff_upstream: Whether to log the diff between your current branch and the upstream branch.
            Defaults to False.

    Example:

        ```
        from neptune_scale.api.run import SourceTrackingConfig

        config = SourceTrackingConfig(
            namespace="source_code_info",
            repository="/path/to/repository",
            upload_run_command=False,
            upload_entry_point=True,
            upload_diff_head=True,
            upload_diff_upstream=True,
        )

        run = Run(
            source_tracking_config=config,
            ...,
        )
        ```
    """

    namespace: str = "source_code"
    repository: Optional[Path] = None
    upload_run_command: bool = True
    upload_entry_point: bool = False
    upload_diff_head: bool = False
    upload_diff_upstream: bool = False


class Run(AbstractContextManager):
    """
    Representation of tracked metadata.
    """

    def __init__(
        self,
        *,
        run_id: Optional[str] = None,
        project: Optional[str] = None,
        api_token: Optional[str] = None,
        mode: Optional[Literal["async", "offline", "disabled"]] = None,
        experiment_name: Optional[str] = None,
        creation_time: Optional[datetime] = None,
        fork_run_id: Optional[str] = None,
        fork_step: Optional[Union[int, float]] = None,
        log_directory: Optional[Union[str, Path]] = None,
        max_queue_size: Optional[int] = None,
        async_lag_threshold: Optional[float] = None,
        on_async_lag_callback: Optional[Callable[[], None]] = None,
        on_queue_full_callback: Optional[Callable[[BaseException, Optional[float]], None]] = None,
        on_network_error_callback: Optional[Callable[[BaseException, Optional[float]], None]] = None,
        on_error_callback: Optional[Callable[[BaseException, Optional[float]], None]] = None,
        on_warning_callback: Optional[Callable[[BaseException, Optional[float]], None]] = None,
        enable_console_log_capture: bool = True,
        runtime_namespace: Optional[str] = None,
        source_tracking_config: Optional[SourceTrackingConfig] = SourceTrackingConfig(),
        **kwargs: Any,
    ) -> None:
        """
        Initializes a run that logs the model-building metadata to Neptune.

        Args:
            run_id: Unique identifier of a run. Must be unique within the project. Max length: 128 bytes. If not provided, a random, human-readable run ID is generated.
            project: Name of the project where the metadata is logged, in the form `workspace-name/project-name`.
                If not provided, the value of the `NEPTUNE_PROJECT` environment variable is used.
            api_token: Your Neptune API token. If not provided, the value of the `NEPTUNE_API_TOKEN` environment
                variable is used.
            mode: Mode of operation. If set to "offline", metadata are stored locally. If set to "disabled", the run doesn't log any metadata.
            experiment_name: If creating a run as an experiment, name (ID) of the experiment to be associated with the run.
            creation_time: Custom creation time of the run.
            fork_run_id: If forking from an existing run, ID of the run to fork from.
            fork_step: If forking from an existing run, step number to fork from.
            log_directory: The base directory where the run's database will be stored. If the path is absolute,
                it is used as provided. If the path is relative, it is treated as relative to the current
                working directory. If set to None, the default is `.neptune` in the current working directory.
            max_queue_size: Deprecated.
            async_lag_threshold: Threshold for the duration between the queueing and synchronization of an operation
                (in seconds). If the duration exceeds the threshold, the callback function is triggered.
            on_async_lag_callback: Callback function triggered when the duration between the queueing and synchronization
            on_queue_full_callback: Callback function triggered when the queue is full. The function should take the exception
                that made the queue full as its argument and an optional timestamp of the last time the exception was raised.
                Deprecated.
            on_network_error_callback: Callback function triggered when a network error occurs.
            on_error_callback: The default callback function triggered when error occurs. It applies if an error
                wasn't caught by other callbacks.
            on_warning_callback: Callback function triggered when a warning occurs.
            enable_console_log_capture: Whether to capture stdout/stderr and log them to Neptune.
            runtime_namespace: Attribute path prefix for the captured logs. If not provided, defaults to "runtime".
            source_tracking_config: Change or disable the logging of source code and Git information. To specify what
                information to log, pass a `SourceTrackingConfig` object. To disable logging, set to `None`.
        """

        if run_id is None:
            run_id = generate_run_id()

        self._fork_step = fork_step

        verify_type("run_id", run_id, str)
        verify_type("project", project, (str, type(None)))
        verify_type("api_token", api_token, (str, type(None)))
        verify_type("experiment_name", experiment_name, (str, type(None)))
        verify_type("creation_time", creation_time, (datetime, type(None)))
        verify_type("fork_run_id", fork_run_id, (str, type(None)))
        verify_type("fork_step", fork_step, (int, float, type(None)))
        verify_type("log_directory", log_directory, (str, Path, type(None)))
        verify_type("async_lag_threshold", async_lag_threshold, (int, float, type(None)))
        verify_type("on_async_lag_callback", on_async_lag_callback, (Callable, type(None)))
        verify_type("on_queue_full_callback", on_queue_full_callback, (Callable, type(None)))
        verify_type("on_network_error_callback", on_network_error_callback, (Callable, type(None)))
        verify_type("on_error_callback", on_error_callback, (Callable, type(None)))
        verify_type("on_warning_callback", on_warning_callback, (Callable, type(None)))
        verify_type("enable_console_log_capture", enable_console_log_capture, bool)
        verify_type("runtime_namespace", runtime_namespace, (str, type(None)))

        system_namespace: Optional[str] = kwargs.get("system_namespace")
        if system_namespace is not None:
            logger.warning(
                "`system_namespace` is deprecated and will be removed in a future version. "
                "Use `runtime_namespace` instead."
            )
            verify_type("system_namespace", system_namespace, (str, type(None)))

        runtime_namespace = system_namespace if runtime_namespace is None else runtime_namespace

        resume = kwargs.get("resume")
        if resume is not None:
            logger.warning(
                "`resume` is deprecated and will be removed in a future version. "
                "Runs are now resumed by default if they exist."
            )

        if max_queue_size is not None:
            logger.warning("`max_queue_size` is deprecated and will be removed in a future version.")

        if (fork_run_id is None) != (fork_step is None):
            raise ValueError("`fork_run_id` and `fork_step` must be used together.")
        if (on_async_lag_callback is None) != (async_lag_threshold is None):
            raise ValueError("`on_async_lag_callback` must be used with `async_lag_threshold`.")

        project = project or os.environ.get(PROJECT_ENV_NAME)
        if project is None:
            raise NeptuneProjectNotProvided()
        verify_project_qualified_name("project", project)

        mode = mode or os.environ.get(MODE_ENV_NAME, "async")  # type: ignore

        verify_non_empty("run_id", run_id)
        verify_max_length("run_id", run_id, MAX_RUN_ID_LENGTH)

        if experiment_name is not None:
            verify_non_empty("experiment_name", experiment_name)
            verify_max_length("experiment_name", experiment_name, MAX_EXPERIMENT_NAME_LENGTH)

        if fork_run_id is not None:
            verify_non_empty("fork_run_id", fork_run_id)
            verify_max_length("fork_run_id", fork_run_id, MAX_RUN_ID_LENGTH)

        # This flag is used to signal that we're closed or being closed (and most likely waiting for sync), and no
        # new data should be logged.
        self._is_closing = False

        self._project: str = project
        self._run_id: str = run_id
        self._experiment_name = experiment_name
        self._api_token = api_token or os.environ.get(API_TOKEN_ENV_NAME)

        self._lock = threading.RLock()

        if mode in ("offline", "async"):
            log_directory = log_directory or os.getenv(envs.LOG_DIRECTORY)
            self._storage_directory_path: Optional[Path] = _resolve_run_storage_directory_path(
                self._project, self._run_id, log_directory
            )
            self._storage_directory_path.mkdir(parents=True, exist_ok=True)

            self._operations_repository_path = self._storage_directory_path / "run_operations.sqlite3"
            self._operations_repo: Optional[OperationsRepository] = OperationsRepository(
                db_path=self._operations_repository_path,
            )
            self._operations_repo.init_db()

            existing_metadata = self._operations_repo.get_metadata()
            if existing_metadata is not None:
                raise NeptuneDatabaseConflict(path=self._operations_repository_path.name)
            self._operations_repo.save_metadata(self._project, self._run_id)

            self._console_log_capture: Optional[ConsoleLogCaptureThread] = (
                None
                if not enable_console_log_capture
                else ConsoleLogCaptureThread(
                    run_id=run_id,
                    runtime_namespace=runtime_namespace.rstrip("/") if runtime_namespace else "runtime",
                    initial_step=fork_step if fork_step is not None else 0,
                    logs_flush_frequency_sec=1,
                    logs_sink=lambda data, step, timestamp: self._log(
                        timestamp=timestamp, step=step, string_series=data
                    ),
                )
            )
            self._logging_enabled = True
        else:
            self._storage_directory_path = None
            self._operations_repo = None
            self._console_log_capture = None
            self._logging_enabled = False

        if mode == "async":
            if self._api_token is None:
                raise NeptuneApiTokenNotProvided()
            assert self._operations_repo is not None

            spawn_mp_context = multiprocessing.get_context("spawn")

            self._errors_monitor: Optional[ErrorsMonitor] = ErrorsMonitor(
                operations_repository=self._operations_repo,
                on_network_error_callback=on_network_error_callback,
                on_error_callback=on_error_callback,
                on_warning_callback=on_warning_callback,
            )

            self._sync_process: Optional[BaseProcess] = spawn_mp_context.Process(
                name="SyncProcess",
                target=run_sync_process,
                kwargs={
                    "project": self._project,
                    "family": self._run_id,
                    "operations_repository_path": self._operations_repository_path,
                    "api_token": self._api_token,
                },
            )
            self._sync_process_supervisor: Optional[ProcessSupervisor] = ProcessSupervisor(
                self._sync_process, self._handle_sync_process_death
            )

            self._lag_tracker: Optional[LagTracker] = None
            if async_lag_threshold is not None and on_async_lag_callback is not None:
                self._lag_tracker = LagTracker(
                    operations_repository=self._operations_repo,
                    async_lag_threshold=async_lag_threshold,
                    on_async_lag_callback=on_async_lag_callback,
                )
                self._lag_tracker.start()

            self._errors_monitor.start()
            with self._lock:
                # This try-except block is to guard against creating the Run() object without
                # the `if __name__ == '__main__':` guard, which is required when using the 'spawn' start method.
                # With 'spawn', the child process will try to import the module and in doing so,
                # initialize the Run() again, which would lead to an infinite stack of starting sync processes.
                # The multiprocessing library guards against this, and we just add a more user-friendly error message.
                try:
                    self._sync_process.start()
                except RuntimeError as e:
                    message = (
                        "Failed to start the synchronization process. "
                        + "This might indicate that Run() was initialized without "
                        + "the if __name__ == '__main__': guard."
                    )
                    raise RuntimeError(message) from e
                self._sync_process_supervisor.start()
        else:
            self._errors_monitor = None
            self._sync_process = None
            self._sync_process_supervisor = None
            self._lag_tracker = None

        self._exit_func: Optional[Callable[[], None]] = atexit.register(self._close)

        if mode != "disabled":
            self._create_run(
                creation_time=datetime.now() if creation_time is None else creation_time,
                experiment_name=experiment_name,
                fork_run_id=fork_run_id,
                fork_step=fork_step,
            )
            self._write_source_tracking_attributes(source_tracking_config)

            if mode == "async":
                self._print_run_info()

        # enable stdout / stderr logging at the end of the constructor, when everything is ready
        if self._console_log_capture is not None:
            self._console_log_capture.start()

    def _handle_sync_process_death(self) -> None:
        with self._lock:
            if not self._is_closing:
                if self._operations_repo is not None:
                    self._operations_repo.save_errors(errors=[NeptuneSynchronizationStopped()])

    def _print_run_info(self) -> None:
        try:
            logger.info(f"View your run in the app:\n{self.get_run_url()}")
        except Exception as e:
            logger.debug(f"Error while printing run info: {e}")

    def _close(self, *, timeout: Optional[float] = None) -> None:
        timer = Timer(timeout)

        # Console log capture is actually a produced of logged data, so let's flush it before closing.
        # This allows to log tracebacks of the potential exception that caused the run to fail.
        if self._console_log_capture is not None:
            self._console_log_capture.interrupt(remaining_iterations=0 if timer.is_expired() else 1)
            self._console_log_capture.join()

        with self._lock:
            if self._is_closing:
                return

            self._is_closing = True

            logger.debug(f"Run is closing, timeout={timeout}")

        if self._sync_process_supervisor is not None:
            self._sync_process_supervisor.interrupt()
            self._sync_process_supervisor.join(timeout=timer.remaining_time())

        if self._sync_process is not None and self._sync_process.is_alive():
            self.wait_for_processing(timeout=timer.remaining_time())

            self._sync_process.terminate()
            self._sync_process.join(timeout=timer.remaining_time())

        if self._lag_tracker is not None:
            self._lag_tracker.interrupt()
            self._lag_tracker.join(timeout=timer.remaining_time())

        if self._errors_monitor is not None:
            self._errors_monitor.interrupt(remaining_iterations=0 if timer.is_expired() else 1)

            # Don't call join() if being called from the error thread, as this will
            # result in a "cannot join current thread" exception.
            if threading.current_thread() != self._errors_monitor:
                self._errors_monitor.join(timeout=timer.remaining_time())

        if self._operations_repo is not None:
            self._operations_repo.close(cleanup_files=True)

        if self._storage_directory_path is not None:
            try:
                logger.debug(f"Removing operations repository directory {self._storage_directory_path}")
                self._storage_directory_path.rmdir()
            except Exception as e:
                logger.debug(f"Failed to remove operations repository directory: {e}")
                logger.warning(
                    f"The directory containing logged metadata was not deleted. "
                    f"This means that some data may have failed to sync. "
                    f"To finish syncing the data, run: `neptune sync {self._operations_repository_path}`"
                )

    def terminate(self) -> None:
        """
        Closes the connection and aborts all synchronization mechanisms.

        Use in error callbacks to stop the run from interfering with the training process
        in case of an unrecoverable error.

        Example:
            ```
            from neptune_scale import Run

            def my_error_callback(exc):
                run.terminate()

            run = Run(
                on_error_callback=my_error_callback
                ...,
            )
            ```
        """

        if self._exit_func is not None:
            atexit.unregister(self._exit_func)
            self._exit_func = None
        self._close(timeout=0)

    def close(self, timeout: Optional[float] = None) -> None:
        """
        Closes the connection to Neptune and waits for data synchronization to be completed.

        Use to finalize a regular run your model-training script.

        Example:
            ```
            from neptune_scale import Run

            with Run(...) as run:
                # logging and training code

                run.close()
            ```
        """

        if self._exit_func is not None:
            atexit.unregister(self._exit_func)
            self._exit_func = None
        self._close(timeout=timeout)

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.close()

    def _create_run(
        self,
        creation_time: datetime,
        experiment_name: Optional[str],
        fork_run_id: Optional[str],
        fork_step: Optional[Union[int, float]],
    ) -> None:
        if self._operations_repo is None:
            logger.debug("Run is in mode that doesn't support creating runs.")
            return

        fork_point: Optional[ForkPoint] = None
        if fork_run_id is not None and fork_step is not None:
            fork_point = ForkPoint(
                parent_project=self._project,
                parent_run_id=fork_run_id,
                step=make_step(number=fork_step),
            )

        create_run = CreateRun(
            family=self._run_id,
            fork_point=fork_point,
            experiment_id=experiment_name,
            creation_time=None if creation_time is None else datetime_to_proto(creation_time),
        )

        self._operations_repo.save_create_run(create_run)

    def _write_source_tracking_attributes(self, source_tracking_config: Optional[SourceTrackingConfig]) -> None:
        if source_tracking_config is None:
            return

        namespace = source_tracking_config.namespace
        repository_info = source_tracking.read_repository_info(
            source_tracking_config.repository,
            entry_point=source_tracking_config.upload_entry_point,
            head_diff=source_tracking_config.upload_diff_head,
            upstream_diff=source_tracking_config.upload_diff_upstream,
            run_command=source_tracking_config.upload_run_command,
        )
        if repository_info is not None:
            data = {
                f"{namespace}/commit/commit_id": repository_info.commit_id,
                f"{namespace}/commit/message": repository_info.commit_message,
                f"{namespace}/commit/author_name": repository_info.commit_author_name,
                f"{namespace}/commit/author_email": repository_info.commit_author_email,
                f"{namespace}/commit/commit_date": repository_info.commit_date,
                f"{namespace}/branch": repository_info.branch,
                **{
                    sanitize_attribute_path(f"{namespace}/remote/{name}"): url
                    for name, url in repository_info.remotes.items()
                },
                f"{namespace}/dirty": repository_info.dirty,
            }

            if repository_info.run_command:
                data[f"{namespace}/run_command"] = repository_info.run_command

            self.log_configs(data=data)

            source_files: dict[str, Union[str, bytes, Path, File]] = {}
            if repository_info.entry_point_path is not None:
                source_files[f"{namespace}/entry_point"] = repository_info.entry_point_path
            if repository_info.head_diff_content is not None:
                source_files[f"{namespace}/diff/head"] = File(
                    source=repository_info.head_diff_content, mime_type="application/patch"
                )
            if repository_info.upstream_diff_content is not None:
                source_files[f"{namespace}/diff/{repository_info.upstream_diff_commit_id}"] = File(
                    source=repository_info.upstream_diff_content, mime_type="application/patch"
                )
            if source_files:
                self.assign_files(files=source_files)

    def log_metrics(
        self,
        data: dict[str, Union[float, int]],
        step: Union[float, int],
        *,
        timestamp: Optional[datetime] = None,
        preview: bool = False,
        preview_completion: Optional[float] = None,
    ) -> None:
        """
        Logs the specified metrics to a Neptune run.

        You can log metrics representing a series of numeric values. Pass the metadata as a dictionary {key: value} with

        - key: path to where the metadata should be stored in the run.
        - value: a float or int value to append to the series.

        For example, {"metrics/accuracy": 0.89}.
        In the attribute path, each forward slash "/" nests the attribute under a namespace.
        Use namespaces to structure the metadata into meaningful categories.

        Args:
            data: Dictionary of metrics to log.
                Each metric value is associated with a step.
                To log multiple metrics at once, pass multiple key-value pairs.
            step: Index of the log entry. Must be increasing unless preview is set to True.
                Tip: Using float rather than int values can be useful, for example, when logging substeps in a batch.
            timestamp (optional): Time of logging the metadata. If not provided, the current time is used. If provided,
                and `timestamp.tzinfo` is not set, the time is assumed to be in the local timezone.
            preview (optional): If set marks the logged metrics as preview values. Preview values may later be overwritten
                by another preview or by a non-preview value. Writing another preview for the same step overrides the previous
                value, even if preview_completion is lower than previous. Previews may no longer be logged after a non-preview
                value is logged for a given metric with greator or equal step.
            preview_completion (optional): A value between 0 and 1 that marks the level of completion of the calculation
                that produced the preview value (higher values mean calculation closer to being complete). Ignored if preview
                is set to False.
                Note: Metric preview with preview_completion of 1.0 is still considered a preview and can be overwritten.
                      The final value of a metric for a given step needs to be logged with preview=False.

        Examples:
            ```
            from neptune_scale import Run

            with Run(...) as run:
                run.log_metrics(
                    data={"loss": 0.14, "acc": 0.78},
                    step=1.2,
                )
            ```
        """
        self._log(
            timestamp=timestamp,
            step=step,
            metrics=Metrics(
                data=data,
                preview=preview,
                preview_completion=preview_completion,
            ),
        )

    @staticmethod
    def _flatten(data: Any) -> dict[str, Any]:
        flattened = {}

        def _flatten_inner(d: Any, prefix: str = "") -> None:
            if is_dataclass(d):
                d = asdict(d)  # type: ignore
            if not isinstance(d, Mapping):
                raise TypeError(f"Cannot flatten value of type {type(d)}. Try `flatten=False`.")
            for key, value in d.items():
                str_key = str(key)
                new_key = f"{prefix}/{str_key}" if prefix else str_key
                if isinstance(value, Mapping) or is_dataclass(value):
                    _flatten_inner(value, prefix=new_key)
                else:
                    flattened[new_key] = value

        _flatten_inner(data)
        return flattened

    @staticmethod
    def _cast_unsupported(
        data: Any,
    ) -> dict[str, Union[str, float, int, bool, datetime, list[str], set[str], tuple[str, ...]]]:
        result: dict[str, Union[str, float, int, bool, datetime, list[str], set[str], tuple[str, ...]]] = {}

        for k, v in data.items():
            if (
                isinstance(v, (float, bool, int, str, datetime))
                or isinstance(v, (list, set, tuple))
                and all(isinstance(item, str) for item in v)
            ):
                result[k] = v
            else:
                result[k] = "" if v is None else str(v)  # If value is None, store as empty string
        return result

    def log_configs(
        self,
        data: Optional[
            Union[
                Mapping[
                    str,
                    Union[str, float, int, bool, datetime, list[Any], set[Any], tuple[Any, ...]],
                ],
                Any,
            ]
        ],
        flatten: bool = False,
        cast_unsupported: bool = False,
    ) -> None:
        """
        Logs the specified metadata to a Neptune run.

        You can log configurations or other single values.
        Pass the data as a dataclass or a mapping-like object, where:
        - keys are strings specifying the path to the metadata in the run structure.
        - values are the configurations or other single values to log.

        For example, {"parameters/learning_rate": 0.001}.
        In the attribute path, each forward slash "/" nests the attribute under a namespace.
        Use namespaces to structure the metadata into meaningful categories.

        Args:
            data: Configs or other values to log, as a `dict`, dataclass, or other mapping-like object with an `.items()` method.
                Available types: float, integer, Boolean, string, and datetime.
                Any `datetime` values that don't have the `tzinfo` attribute set are assumed to be in the local timezone.
            flatten: Flattens nested dictionaries and dataclasses before logging. Defaults to False.
            cast_unsupported: Casts unsupported types to strings before logging. Defaults to False.


        Example:
            ```
            from neptune_scale import Run

            with Run(...) as run:
                run.log_configs(
                    data={
                        "parameters/learning_rate": 0.001,
                        "parameters/batch_size": 64,
                    },
                )
            ```
        """
        if data is None:
            return

        if is_dataclass(data):
            data = asdict(data)  # type: ignore
        elif not isinstance(data, Mapping):
            raise TypeError(
                f"configs must be a mapping-like object with an `.items()` method (e.g., `dict`) or a `dataclass` instance, or `NoneType` (was {type(data)})"
            )

        if flatten:
            data = Run._flatten(data)

        if cast_unsupported:
            data = Run._cast_unsupported(data)

        data = cast(
            dict[str, Union[str, float, int, bool, datetime, list[str], set[str], tuple[str, ...]]],
            data,
        )
        self._log(configs=data)

    def log_string_series(
        self,
        data: dict[str, str],
        step: Union[float, int],
        *,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """
        Logs the specified string series to a Neptune run.

        Pass the metadata as a dictionary {key: value} with:

        - key: path to where the metadata should be stored in the run.
        - value: a string value to append to the series

        For example, {"series/log": "message"}.

        In the attribute path, each forward slash "/" nests the attribute under a namespace.
        Use namespaces to structure the metadata into meaningful categories.

        Args:
            data: Dictionary of strings to log.
                Each string value is associated with a step. To log multiple strings at once, pass multiple key-value
                pairs.
            step: Index of the log entry. Must be increasing unless preview is set to True.
                Tip: Using float rather than int values can be useful, for example, when logging substeps in a batch.
            timestamp (optional): Time of logging the metadata. If not provided, the current time is used. If provided,
                and `timestamp.tzinfo` is not set, the time is assumed to be in the local timezone.

        Examples:
            ```
            from neptune_scale import Run

            with Run(...) as run:
                run.log_string_series(
                    data={"messages/errors": "error message", "messages/info": "Training completed"},
                    step=1.2,
                )
            ```
        """
        self._log(timestamp=timestamp, step=step, string_series=data)

    def log_histograms(
        self,
        histograms: dict[str, Histogram],
        step: Union[float, int],
        *,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """Logs the specified histograms at a particular step.

        Args:
            histograms (dict[str, Histogram]):
                A dictionary with attribute names as keys and histogram objects as values.
            step (float or int): Index of the series entry.
                Tip: Using float rather than int values can be useful, for example, when logging substeps in a batch.
            timestamp (datetime, optional): Time of logging the histograms data.
                If not provided, the current time is used.
                If provided, and `timestamp.tzinfo` is not set, the local timezone is used.

        Example:
            ```
            from neptune_scale import Run
            from neptune_scale.types import Histogram


            run = Run(...)

            # for step in iteration
            my_histogram = Histogram(
                bin_edges=[0, 1, 40, 89, 1000],
                counts=[5, 82, 44, 1],
            )
            run.log_histograms(
                histograms={
                    "layers/1/activations": my_histogram
                },
                step=1,
            )
            ```
        """
        self._log(timestamp=timestamp, histograms=histograms, step=step)

    def add_tags(self, tags: Union[list[str], set[str], tuple[str]], group_tags: bool = False) -> None:
        """
        Adds the list of tags to the run.

        Args:
            tags: Tags to add to the run, as a list or set of strings.
            group_tags: To add group tags instead of regular tags, set to `True`.

        Example:
            ```
            from neptune_scale import Run

            with Run(...) as run:
                run.add_tags(tags=["tag1", "tag2", "tag3"])
            ```
        """
        name = "sys/tags" if not group_tags else "sys/group_tags"
        self._log(tags_add={name: tags})

    def remove_tags(self, tags: Union[list[str], set[str], tuple[str]], group_tags: bool = False) -> None:
        """
        Removes the specified tags from the run.

        Args:
            tags: Tags to remove to the run, as a list or set of strings.
            group_tags: To remove group tags instead of regular tags, set to `True`.

        Example:
            ```
            from neptune_scale import Run

            with Run(...) as run:
                run.remove_tags(tags=["tag2", "tag3"])
            ```
        """
        name = "sys/tags" if not group_tags else "sys/group_tags"
        self._log(tags_remove={name: tags})

    def assign_files(self, files: dict[str, Union[str, Path, bytes, File]]) -> None:
        """
        Assigns single file values to the specified attributes and uploads files' contents.
        Existing values for are overwritten.

        If a value is a string or Path, it is treated as a file path.
        If a value is bytes, it is treated as raw file content to save.
        If a value is a `File` object, its `source` field is used (string and Path are treated as file paths,
        bytes are treated as raw file content to save).

        The files are uploaded to the Neptune object storage and the attribute are set to point to the uploaded files.

        Mime type and size are determined from the provided source (file or bytes buffer) automatically,
        but this mechanism can be overridden by providing `mime_type` and `size_bytes` fields in the `File` object.

        Args:
            files: dictionary of files to log, where values are one of: str, Path, bytes, or `File` objects
        """
        self._log(files=files)

    def log_files(
        self,
        files: dict[str, Union[str, Path, bytes, File]],
        step: Union[float, int],
        *,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """Appends a file value and uploads the file contents at a particular step.

        Pass the data as a dictionary {key: value} with:

        - key: path to the attribute where the series is stored in the run.
        - value: a file value to append to the series.

        The files are uploaded to the Neptune object storage and the attributes are set to point to the uploaded files.

        Mime type and size are determined from the provided source (file or bytes buffer) automatically.
        You can override this by providing `mime_type` and `size_bytes` fields in the `File` object.

        Args:
            files: dictionary of files to log, where values are one of: str, Path, bytes, or `File` objects.
                To log multiple file series for a step in a single function call, include multiple key-value pairs
                in the dictionary.
                If a value is a string or Path, it's treated as a file path.
                If a value is bytes, it's treated as raw file content to save.
                If a value is a `File` object, its `source` field is used.
            step (float or int): Index of the series entry.
                Tip: Using float rather than int values can be useful, for example, when logging substeps in a batch.
            timestamp (datetime, optional): Time of logging the files.
                If not provided, the current time is used.
                If provided, and `timestamp.tzinfo` is not set, the local timezone is used.

        Example:
            ```
            from neptune_scale import Run

            run = Run(...)

            # for step in training loop
            run.log_files(
                files={"predictions/train": "output/train/predictions.png"},
                step=1,
            )
            ```
        """
        self._log(timestamp=timestamp, step=step, file_series=files)

    def log(
        self,
        step: Optional[Union[float, int]] = None,
        timestamp: Optional[datetime] = None,
        configs: Optional[
            dict[str, Union[str, float, int, bool, datetime, list[str], set[str], tuple[str, ...]]]
        ] = None,
        metrics: Optional[dict[str, Union[float, int]]] = None,
        tags_add: Optional[dict[str, Union[list[str], set[str], tuple[str]]]] = None,
        tags_remove: Optional[dict[str, Union[list[str], set[str], tuple[str]]]] = None,
    ) -> None:
        """
        This method is retained for backward compatibility.
        See one of the following instead:

        - log_configs()
        - log_metrics()
        - add_tags()
        - remove_tags()
        """
        self._log(
            timestamp=timestamp,
            step=step,
            configs=configs,
            metrics=Metrics(data=metrics) if metrics is not None else None,
            tags_add=tags_add,
            tags_remove=tags_remove,
        )

    def _log(
        self,
        timestamp: Optional[datetime] = None,
        step: Optional[Union[float, int]] = None,
        configs: Optional[
            dict[str, Union[str, float, int, bool, datetime, list[str], set[str], tuple[str, ...]]]
        ] = None,
        metrics: Optional[Metrics] = None,
        files: Optional[dict[str, Union[str, Path, bytes, File]]] = None,
        string_series: Optional[dict[str, str]] = None,
        file_series: Optional[dict[str, Union[str, Path, bytes, File]]] = None,
        histograms: Optional[dict[str, Histogram]] = None,
        tags_add: Optional[dict[str, Union[list[str], set[str], tuple[str]]]] = None,
        tags_remove: Optional[dict[str, Union[list[str], set[str], tuple[str]]]] = None,
    ) -> None:
        verify_type("timestamp", timestamp, (datetime, type(None)))
        verify_type("configs", configs, (dict, type(None)))
        verify_type("tags_add", tags_add, (dict, type(None)))
        verify_type("tags_remove", tags_remove, (dict, type(None)))
        verify_type("files", files, (dict, type(None)))

        if metrics is not None:
            verify_type("step", step, (float, int, type(None)))
            verify_type("metrics", metrics, Metrics)
            verify_type("metrics", metrics.data, dict)
            verify_type("preview", metrics.preview, bool)
            verify_type("preview_completion", metrics.preview_completion, (float, type(None)))

            if not metrics.preview:
                if metrics.preview_completion not in (None, 1.0):
                    raise ValueError("preview_completion can only be specified for metric previews")
                # we don't send info about preview if preview=False
                # and dropping 1.0 (even if it's technically a correct value)
                # reduces chance of errors down the line
                metrics.preview_completion = None
            if metrics.preview_completion is not None:
                verify_value_between("preview_completion", metrics.preview_completion, 0.0, 1.0)

        if string_series is not None:
            verify_type("step", step, (float, int))
            verify_type("string_series", string_series, dict)

        if file_series is not None:
            verify_type("step", step, (float, int))
            verify_type("file_series", file_series, dict)

        if histograms is not None:
            verify_type("histograms", histograms, dict)
            verify_type("step", step, (float, int))

        # Don't log anything after we've been stopped. This allows continuing the training script
        # after a non-recoverable error happened. Note we don't to use self._lock in this check,
        # to keep the common path faster, because the benefit of locking here is minimal.
        if self._is_closing:
            return

        if not self._logging_enabled:
            return

        assert self._operations_repo is not None

        if timestamp is None:
            timestamp = datetime.now()
        elif isinstance(timestamp, float):
            timestamp = datetime.fromtimestamp(timestamp)

        file_upload_requests = self._prepare_files_for_upload(files)
        file_data = (
            {attr_name: self._file_request_to_file_ref_data(req) for attr_name, req in file_upload_requests}
            if file_upload_requests
            else None
        )
        file_series_upload_requests = self._prepare_files_for_upload(file_series, step=step)
        file_series_data = (
            {attr_name: self._file_request_to_file_ref_data(req) for attr_name, req in file_series_upload_requests}
            if file_series_upload_requests
            else None
        )
        all_file_upload_requests = file_upload_requests + file_series_upload_requests

        splitter: MetadataSplitter = MetadataSplitter(
            project=self._project,
            run_id=self._run_id,
            timestamp=timestamp,
            step=step,
            configs=configs,
            metrics=metrics,
            files=file_data,
            file_series=file_series_data,
            add_tags=tags_add,
            remove_tags=tags_remove,
        )

        operations = list(
            itertools.chain(
                splitter,
                string_series_to_update_run_snapshots(string_series=string_series, step=step, timestamp=timestamp),
                histograms_to_update_run_snapshots(histograms=histograms, step=step, timestamp=timestamp),
            )
        )

        # Save file upload requests only after MetadataSplitter processed input
        if all_file_upload_requests:
            self._operations_repo.save_file_upload_requests([req for _, req in all_file_upload_requests])

        self._operations_repo.save_update_run_snapshots(operations)

    @staticmethod
    def _file_request_to_file_ref_data(file_request: FileUploadRequest) -> FileRefData:
        return FileRefData(
            destination=file_request.destination,
            mime_type=file_request.mime_type,
            size_bytes=file_request.size_bytes,
        )

    def _prepare_files_for_upload(
        self,
        files: Optional[dict[str, Union[str, Path, bytes, File]]],
        step: Optional[Union[float, int]] = None,
    ) -> list[tuple[str, FileUploadRequest]]:
        """Process user input to produce a list of (attribute-name, FileUploadRequest tuples)

        All buffers passed as arguments are saved to disk in the run's storage directory.
        Mime types and file sizes are populated if not provided by the user.

        Any errors during processing are logged with warning and the file is skipped.
        """
        if files is None:
            return []

        result = []
        for attr_name, file in files.items():
            try:
                if isinstance(file, File):
                    source, mime_type, destination, size = (
                        file.source,
                        file.mime_type,
                        file.destination,
                        file.size,
                    )
                else:
                    source = file
                    mime_type = size = destination = None

                if isinstance(source, bytes):
                    size = size or len(source)
                    mime_type = mime_type or guess_mime_type_from_bytes(source, destination)
                    file_path = _save_buffer_to_disk(
                        self._storage_directory_path,  # type: ignore
                        source,
                        mimetypes.guess_extension(mime_type) if mime_type else None,
                    )
                    logger.debug(f"Saved temporary file {file_path} for attribute `{attr_name}`")
                elif isinstance(source, (str, Path)):
                    file_path = Path(source)
                    mime_type = mime_type or guess_mime_type_from_file(file_path, destination)
                    if mime_type is None:
                        raise ValueError(f"Cannot determine mime type for file '{file_path}'")
                    size = size or file_path.stat().st_size
                else:
                    logger.warning(f"Skipping file attribute `{attr_name}`: Unsupported type {type(file)}")
                    continue

                assert mime_type is not None
                request = FileUploadRequest(
                    source_path=str(file_path.absolute()),
                    destination=(
                        generate_destination(self._run_id, attr_name, file_path.name, step)
                        if destination is None
                        else str(destination)
                    ),
                    mime_type=str(mime_type),
                    size_bytes=int(size),
                    is_temporary=isinstance(source, bytes),
                )
                result.append((attr_name, request))
            except Exception as e:
                logger.warning(f"Skipping file attribute `{attr_name}`: {e}")
                continue

        return result

    def wait_for_submission(self, timeout: Optional[float] = None, verbose: bool = True) -> bool:
        """
        Waits until all metadata is submitted to Neptune for processing.

        When submitted, the data is not yet saved in Neptune until fully processed.
        See wait_for_processing().

        Args:
            timeout (float, optional): In seconds, the maximum time to wait for submission.
            verbose (bool): If True (default), prints messages about the waiting process.
        """
        timer = Timer(timeout)
        submission_status = self._wait_for_operation_submission(timeout=timer.remaining_time(), verbose=verbose)
        files_status = self._wait_for_file_upload(timeout=timer.remaining_time(), verbose=verbose)
        return submission_status and files_status

    def wait_for_processing(self, timeout: Optional[float] = None, verbose: bool = True) -> bool:
        """
        Waits until all metadata is processed by Neptune.

        Once the call is complete, the data is saved in Neptune.

        Args:
            timeout (float, optional): In seconds, the maximum time to wait for processing.
            verbose (bool): If True (default), prints messages about the waiting process.
        """
        timer = Timer(timeout)
        processing_status = self._wait_for_operation_processing(timeout=timer.remaining_time(), verbose=verbose)
        file_status = self._wait_for_file_upload(timeout=timer.remaining_time(), verbose=verbose)
        return processing_status and file_status

    def _wait_for_operation_submission(
        self,
        timeout: Optional[float] = None,
        verbose: bool = True,
    ) -> bool:
        if self._operations_repo is None:
            return True

        if verbose:
            logger.info("Waiting for all operations to be processed")

        if timeout is None and verbose:
            logger.warning("No timeout specified. Waiting indefinitely")

        timer = Timer(timeout)
        sleep_time = float(OPERATION_REPOSITORY_POLL_SLEEP_TIME)
        throttled_logger = ThrottledLogger(logger, enabled=verbose)

        while True:
            try:
                with self._lock:
                    if self._sync_process is None or not self._sync_process.is_alive():
                        logger.warning("Waiting interrupted because sync process is not running")
                        timer = Timer(0)  # reset timer to avoid waiting indefinitely

                # assumption: submissions are always behind or equal to the logged operations
                submitted_sequence_id_range = self._operations_repo.get_operation_submission_sequence_id_range()
                logged_sequence_id_range = self._operations_repo.get_operations_sequence_id_range()

                if submitted_sequence_id_range is not None and logged_sequence_id_range is not None:
                    _, max_submitted_sequence_id = submitted_sequence_id_range
                    _, max_logged_sequence_id = logged_sequence_id_range
                    operations_count = max(0, max_logged_sequence_id - max_submitted_sequence_id)

                    if operations_count > 0:
                        throttled_logger.info(
                            "Waiting for remaining %d operation(s) to be submitted",
                            operations_count,
                        )
                elif logged_sequence_id_range is not None:
                    min_logged_sequence_id, max_logged_sequence_id = logged_sequence_id_range
                    operations_count = max(0, max_logged_sequence_id - min_logged_sequence_id + 1)

                    if operations_count > 0:
                        throttled_logger.info(
                            "Waiting. No operations were submitted yet. Operations to sync: %s",
                            operations_count,
                        )
                else:
                    operations_count = 0

                if operations_count > 0:
                    if timer.is_expired():
                        logger.info("Waiting interrupted because timeout was reached")
                        return False
                    sleep_time = min(sleep_time, timer.remaining_time_or_inf())

                    time.sleep(sleep_time)
                else:
                    logger.info("All operations were submitted")
                    return True
            except KeyboardInterrupt:
                logger.warning("Waiting interrupted by user")
                return False

    def _wait_for_operation_processing(
        self,
        timeout: Optional[float] = None,
        verbose: bool = True,
    ) -> bool:
        if self._operations_repo is None:
            return True

        if verbose:
            logger.info("Waiting for all operations to be processed")

        if timeout is None and verbose:
            logger.warning("No timeout specified. Waiting indefinitely")

        operations_count_limit = 10_000
        timer = Timer(timeout)
        sleep_time = float(OPERATION_REPOSITORY_POLL_SLEEP_TIME)
        throttled_logger = ThrottledLogger(logger, enabled=verbose)

        while True:
            try:
                with self._lock:
                    if self._sync_process is None or not self._sync_process.is_alive():
                        logger.warning("Waiting interrupted because sync process is not running")
                        timer = Timer(0)  # reset timer to avoid waiting indefinitely

                operations_count = self._operations_repo.get_operation_count(limit=operations_count_limit)

                if operations_count > 0:
                    throttled_logger.info(
                        "Waiting for remaining %d%s operation(s) to be processed",
                        operations_count,
                        "" if operations_count < operations_count_limit else "+",
                    )

                    if timer.is_expired():
                        logger.info("Waiting interrupted because timeout was reached")
                        return False
                    sleep_time = min(sleep_time, timer.remaining_time_or_inf())

                    time.sleep(sleep_time)
                else:
                    logger.info("All operations were processed")
                    return True
            except KeyboardInterrupt:
                logger.warning("Waiting interrupted by user")
                return False

    def _wait_for_file_upload(
        self,
        timeout: Optional[float] = None,
        verbose: bool = True,
    ) -> bool:
        if self._operations_repo is None:
            return True

        if verbose:
            logger.info("Waiting for all files to be uploaded")

        if timeout is None and verbose:
            logger.warning("No timeout specified. Waiting indefinitely")

        upload_count_limit = 10_000
        timer = Timer(timeout)
        sleep_time = float(OPERATION_REPOSITORY_POLL_SLEEP_TIME)
        throttled_logger = ThrottledLogger(logger, enabled=verbose)

        while True:
            try:
                with self._lock:
                    if self._sync_process is None or not self._sync_process.is_alive():
                        logger.warning("Waiting interrupted because sync process is not running")
                        timer = Timer(0)  # reset timer to avoid waiting indefinitely

                upload_count = self._operations_repo.get_file_upload_requests_count(limit=upload_count_limit)

                if upload_count > 0:
                    throttled_logger.info(
                        "Waiting for remaining %d%s file(s) to be uploaded",
                        upload_count,
                        "" if upload_count < upload_count_limit else "+",
                    )

                    if timer.is_expired():
                        logger.info("Waiting interrupted because timeout was reached")
                        return False
                    sleep_time = min(sleep_time, timer.remaining_time_or_inf())

                    time.sleep(sleep_time)
                else:
                    logger.info("All files were uploaded")
                    return True
            except KeyboardInterrupt:
                logger.warning("Waiting interrupted by user")
                return False

    def get_run_url(self) -> str:
        """
        Returns a URL for viewing the run in the Neptune web app. Requires the API token to be provided
        during run initialization, either through the constructor or the `NEPTUNE_API_TOKEN` environment variable.
        """
        return _get_run_url(self._api_token, self._project, run_id=self._run_id)

    def get_experiment_url(self) -> str:
        """
        Returns a URL for viewing the experiment in the Neptune web app. Requires the API token to be provided
        during run initialization, either through the constructor or the `NEPTUNE_API_TOKEN` environment variable.
        """

        if self._experiment_name is None:
            raise ValueError("`experiment_name` was not provided during Run initialization")

        return _get_run_url(self._api_token, self._project, experiment_name=self._experiment_name)


def _sanitize_path_component(component: str) -> str:
    """Modify a path component to be safe for use in a filesystem:
    - Replace ASCII control characters, and characters invalid/troublesome on some filesystems with an underscore
    - Make sure there are no sequences of more than one underscore in the result
    - Make sure the component is not longer than 64 characters

    Assuming very long run ids and project names, this still leaves room for Windows paths
    which are max 260 characters by default, and makes sure that the filename does not
    exceed 255 characters, which is a common limit on many filesystems.
    """

    result = re.sub(r'[<>:" /\\|?*\x00-\x1F]', "_", component)
    result = re.sub(r"_{2,}", "_", result)

    return result[:64]


def _resolve_run_storage_directory_path(
    project: str, run_id: str, user_provided_log_dir: Optional[Union[str, Path]]
) -> Path:
    """Return an absolute path to a directory where a Run should store its files."""

    sanitized_project = _sanitize_path_component(project)
    sanitized_run_id = _sanitize_path_component(run_id)

    timestamp_ns = int(time.time() * 1e9)
    base_directory = Path(os.getcwd()) / ".neptune" if user_provided_log_dir is None else Path(user_provided_log_dir)

    return (base_directory / f"{sanitized_project}_{sanitized_run_id}_{timestamp_ns}").absolute()


def _get_run_url(
    api_token: Optional[str],
    project_name: str,
    run_id: Optional[str] = None,
    experiment_name: Optional[str] = None,
) -> str:
    assert not (run_id and experiment_name)
    if api_token is None:
        raise NeptuneApiTokenNotProvided()

    base_url = _extract_api_url_from_token(api_token)

    # project_name is validated in Run.__init__(), so it has at least 2 parts
    workspace, project = project_name.split("/", maxsplit=1)
    workspace = quote_plus(workspace)
    project = quote_plus(project)

    if run_id:
        return f"{base_url}/{workspace}/{project}/-/run/?customId={quote_plus(run_id)}"
    else:
        assert experiment_name  # mypy
        return (
            f"{base_url}/{workspace}/{project}/runs/details?runIdentificationKey"
            f"={quote_plus(experiment_name)}&type=experiment"
        )


def _extract_api_url_from_token(api_token: str) -> str:
    try:
        json_data = json.loads(base64.b64decode(api_token))
        return json_data["api_url"].rstrip("/")  # type: ignore
    except (binascii.Error, json.JSONDecodeError, KeyError):
        raise ValueError("Unable to extract Neptune URL from the provided API token")


def _save_buffer_to_disk(directory_path: Path, buffer: bytes, extension: Optional[str]) -> Path:
    """
    Save the buffer to disk in the specified directory. Return the path to the saved file.
    If provided, the extension must be in the format ".ext".
    """
    filename = f"{str(uuid.uuid4())}" + (extension or "")
    file_path = directory_path / filename

    with open(file_path, "wb") as file:
        file.write(buffer)

    return file_path
