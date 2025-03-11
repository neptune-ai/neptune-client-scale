"""
Python package
"""

from __future__ import annotations

import re
from pathlib import Path
from types import TracebackType

from neptune_scale.sync.operations_repository import OperationsRepository

__all__ = ["Run"]

import atexit
import os
import threading
import time
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import datetime
from typing import (
    Any,
    Literal,
    Optional,
    Union,
)

from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import ForkPoint
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import Run as CreateRun

from neptune_scale.api.attribute import AttributeStore
from neptune_scale.api.metrics import Metrics
from neptune_scale.api.validation import (
    verify_dict_type,
    verify_max_length,
    verify_non_empty,
    verify_project_qualified_name,
    verify_type,
)
from neptune_scale.exceptions import (
    NeptuneApiTokenNotProvided,
    NeptuneDatabaseConflict,
    NeptuneProjectNotProvided,
    NeptuneSynchronizationStopped,
)
from neptune_scale.net.serialization import (
    datetime_to_proto,
    make_step,
)
from neptune_scale.sync.errors_tracking import (
    ErrorsMonitor,
    ErrorsQueue,
)
from neptune_scale.sync.lag_tracking import LagTracker
from neptune_scale.sync.parameters import (
    MAX_EXPERIMENT_NAME_LENGTH,
    MAX_RUN_ID_LENGTH,
    MINIMAL_WAIT_FOR_ACK_SLEEP_TIME,
    MINIMAL_WAIT_FOR_PUT_SLEEP_TIME,
    STOP_MESSAGE_FREQUENCY,
)
from neptune_scale.sync.sequence_tracker import SequenceTracker
from neptune_scale.sync.sync_process import SyncProcess
from neptune_scale.util import envs
from neptune_scale.util.envs import (
    API_TOKEN_ENV_NAME,
    MODE_ENV_NAME,
    PROJECT_ENV_NAME,
)
from neptune_scale.util.logger import get_logger
from neptune_scale.util.process_link import ProcessLink
from neptune_scale.util.shared_var import (
    SharedFloat,
    SharedInt,
)

logger = get_logger()


class Run(AbstractContextManager):
    """
    Representation of tracked metadata.
    """

    def __init__(
        self,
        *,
        run_id: str,
        project: Optional[str] = None,
        api_token: Optional[str] = None,
        resume: bool = False,
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
    ) -> None:
        """
        Initializes a run that logs the model-building metadata to Neptune.

        Args:
            run_id: Unique identifier of a run. Must be unique within the project. Max length: 128 bytes.
            project: Name of the project where the metadata is logged, in the form `workspace-name/project-name`.
                If not provided, the value of the `NEPTUNE_PROJECT` environment variable is used.
            api_token: Your Neptune API token. If not provided, the value of the `NEPTUNE_API_TOKEN` environment
                variable is used.
            resume: Whether to resume an existing run.
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
            on_network_error_callback: Callback function triggered when a network error occurs.
            on_error_callback: The default callback function triggered when error occurs. It applies if an error
                wasn't caught by other callbacks.
            on_warning_callback: Callback function triggered when a warning occurs.
        """

        verify_type("run_id", run_id, str)
        verify_type("resume", resume, bool)
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

        if resume and creation_time is not None:
            raise ValueError("`resume` and `creation_time` cannot be used together.")
        if resume and experiment_name is not None:
            raise ValueError("`resume` and `experiment_name` cannot be used together.")
        if (fork_run_id is not None and fork_step is None) or (fork_run_id is None and fork_step is not None):
            raise ValueError("`fork_run_id` and `fork_step` must be used together.")
        if resume and fork_run_id is not None:
            raise ValueError("`resume` and `fork_run_id` cannot be used together.")
        if resume and fork_step is not None:
            raise ValueError("`resume` and `fork_step` cannot be used together.")

        if (
            on_async_lag_callback is not None
            and async_lag_threshold is None
            or on_async_lag_callback is None
            and async_lag_threshold is not None
        ):
            raise ValueError("`on_async_lag_callback` must be used with `async_lag_threshold`.")

        if max_queue_size is not None:
            logger.warning("`max_queue_size` is deprecated and will be removed in a future version.")

        project = project or os.environ.get(PROJECT_ENV_NAME)
        if project:
            project = project.strip('"').strip("'")
        else:
            raise NeptuneProjectNotProvided()
        assert project is not None  # mypy
        input_project: str = project

        mode = mode or os.environ.get(MODE_ENV_NAME, "async")  # type: ignore

        verify_non_empty("run_id", run_id)
        if experiment_name is not None:
            verify_non_empty("experiment_name", experiment_name)
            verify_max_length("experiment_name", experiment_name, MAX_EXPERIMENT_NAME_LENGTH)
        if fork_run_id is not None:
            verify_non_empty("fork_run_id", fork_run_id)
            verify_max_length("fork_run_id", fork_run_id, MAX_RUN_ID_LENGTH)

        verify_project_qualified_name("project", project)

        verify_max_length("run_id", run_id, MAX_RUN_ID_LENGTH)

        # This flag is used to signal that we're closed or being closed (and most likely waiting for sync), and no
        # new data should be logged.
        self._is_closing = False

        self._project: str = input_project
        self._run_id: str = run_id

        self._lock = threading.RLock()

        if mode in ("offline", "async"):
            log_directory = log_directory or os.getenv(envs.LOG_DIRECTORY)
            operations_repository_path = _resolve_run_db_path(self._project, self._run_id, log_directory)
            self._operations_repo: Optional[OperationsRepository] = OperationsRepository(
                db_path=operations_repository_path,
            )
            self._operations_repo.init_db()

            existing_metadata = self._operations_repo.get_metadata()
            if existing_metadata is not None:
                raise NeptuneDatabaseConflict(path=operations_repository_path)
            self._operations_repo.save_metadata(self._project, self._run_id)

            self._sequence_tracker: Optional[SequenceTracker] = SequenceTracker()
            self._attr_store: Optional[AttributeStore] = AttributeStore(
                self._project, self._run_id, self._operations_repo, self._sequence_tracker
            )
        else:
            self._operations_repo = None
            self._sequence_tracker = None
            self._attr_store = None

        if mode == "async":
            assert self._sequence_tracker is not None

            api_token = api_token or os.environ.get(API_TOKEN_ENV_NAME)
            if api_token is None:
                raise NeptuneApiTokenNotProvided()
            assert api_token is not None  # mypy
            input_api_token: str = api_token

            self._errors_queue: Optional[ErrorsQueue] = ErrorsQueue()
            self._errors_monitor: Optional[ErrorsMonitor] = ErrorsMonitor(
                errors_queue=self._errors_queue,
                on_queue_full_callback=on_queue_full_callback,
                on_network_error_callback=on_network_error_callback,
                on_error_callback=on_error_callback,
                on_warning_callback=on_warning_callback,
            )

            self._last_queued_seq: Optional[SharedInt] = SharedInt(-1)
            self._last_ack_seq: Optional[SharedInt] = SharedInt(-1)
            self._last_ack_timestamp: Optional[SharedFloat] = SharedFloat(-1)

            self._process_link: Optional[ProcessLink] = ProcessLink()
            self._sync_process: Optional[SyncProcess] = SyncProcess(
                project=self._project,
                family=self._run_id,
                operations_repository_path=operations_repository_path,
                errors_queue=self._errors_queue,
                process_link=self._process_link,
                api_token=input_api_token,
                last_queued_seq=self._last_queued_seq,
                last_ack_seq=self._last_ack_seq,
                last_ack_timestamp=self._last_ack_timestamp,
            )

            self._lag_tracker: Optional[LagTracker] = None
            if async_lag_threshold is not None and on_async_lag_callback is not None:
                self._lag_tracker = LagTracker(
                    errors_queue=self._errors_queue,
                    sequence_tracker=self._sequence_tracker,
                    last_ack_timestamp=self._last_ack_timestamp,
                    async_lag_threshold=async_lag_threshold,
                    on_async_lag_callback=on_async_lag_callback,
                )
                self._lag_tracker.start()

            self._errors_monitor.start()
            with self._lock:
                self._sync_process.start()
                self._process_link.start(on_link_closed=self._on_child_link_closed)
        else:
            self._errors_queue = None
            self._errors_monitor = None
            self._last_queued_seq = None
            self._last_ack_seq = None
            self._last_ack_timestamp = None
            self._process_link = None
            self._sync_process = None
            self._lag_tracker = None

        self._exit_func: Optional[Callable[[], None]] = atexit.register(self._close)

        if mode != "disabled" and not resume:
            self._create_run(
                creation_time=datetime.now() if creation_time is None else creation_time,
                experiment_name=experiment_name,
                fork_run_id=fork_run_id,
                fork_step=fork_step,
            )

    def _on_child_link_closed(self, _: ProcessLink) -> None:
        with self._lock:
            if not self._is_closing:
                logger.error("The background synchronization process has stopped unexpectedly.")
                if self._errors_queue is not None:
                    self._errors_queue.put(NeptuneSynchronizationStopped())

    def _close(self, *, wait: bool = True) -> None:
        with self._lock:
            if self._is_closing:
                return

            self._is_closing = True

            logger.debug(f"Run is closing, wait={wait}")

        if self._sync_process is not None and self._sync_process.is_alive():
            if wait:
                self.wait_for_processing()

            self._sync_process.terminate()
            self._sync_process.join()

        if self._process_link is not None:
            self._process_link.stop()

        if self._lag_tracker is not None:
            self._lag_tracker.interrupt()
            self._lag_tracker.join()

        if self._errors_monitor is not None:
            self._errors_monitor.interrupt()

            # Don't call join() if being called from the error thread, as this will
            # result in a "cannot join current thread" exception.
            if threading.current_thread() != self._errors_monitor:
                self._errors_monitor.join()

        if self._operations_repo is not None:
            self._operations_repo.close(cleanup_files=True)

        if self._errors_queue is not None:
            self._errors_queue.close()

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
        self._close(wait=False)

    def close(self) -> None:
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
        self._close(wait=True)

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
        if self._operations_repo is None or self._sequence_tracker is None:
            logger.debug("Run is in mode that doesn't support creating runs.")
            return

        fork_point: Optional[ForkPoint] = None
        if fork_run_id is not None and fork_step is not None:
            fork_point = ForkPoint(
                parent_project=self._project, parent_run_id=fork_run_id, step=make_step(number=fork_step)
            )

        create_run = CreateRun(
            family=self._run_id,
            fork_point=fork_point,
            experiment_id=experiment_name,
            creation_time=None if creation_time is None else datetime_to_proto(creation_time),
        )

        sequence = self._operations_repo.save_create_run(create_run)
        self._sequence_tracker.update_sequence_id(sequence)

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
            metrics=Metrics(
                data=data,
                step=step,
                preview=preview,
                preview_completion=preview_completion,
            ),
        )

    def log_configs(
        self, data: Optional[dict[str, Union[float, bool, int, str, datetime, list, set, tuple]]] = None
    ) -> None:
        """
        Logs the specified metadata to a Neptune run.

        You can log configurations or other single values. Pass the metadata as a dictionary {key: value} with

        - key: path to where the metadata should be stored in the run.
        - value: configuration or other single value to log.

        For example, {"parameters/learning_rate": 0.001}.
        In the attribute path, each forward slash "/" nests the attribute under a namespace.
        Use namespaces to structure the metadata into meaningful categories.

        Args:
            data: Dictionary of configs or other values to log.
                Available types: float, integer, Boolean, string, and datetime.

        Any `datetime` values that don't have the `tzinfo` attribute set are assumed to be in the local timezone.

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
        self._log(configs=data)

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

    def log(
        self,
        step: Optional[Union[float, int]] = None,
        timestamp: Optional[datetime] = None,
        configs: Optional[dict[str, Union[float, bool, int, str, datetime, list, set, tuple]]] = None,
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
        mtr = Metrics(step=step, data=metrics) if metrics is not None else None
        self._log(timestamp=timestamp, configs=configs, metrics=mtr, tags_add=tags_add, tags_remove=tags_remove)

    def _log(
        self,
        timestamp: Optional[datetime] = None,
        configs: Optional[dict[str, Union[float, bool, int, str, datetime, list, set, tuple]]] = None,
        metrics: Optional[Metrics] = None,
        tags_add: Optional[dict[str, Union[list[str], set[str], tuple[str]]]] = None,
        tags_remove: Optional[dict[str, Union[list[str], set[str], tuple[str]]]] = None,
    ) -> None:
        verify_type("timestamp", timestamp, (datetime, type(None)))
        verify_type("configs", configs, (dict, type(None)))
        verify_type("metrics", metrics, (Metrics, type(None)))
        verify_type("tags_add", tags_add, (dict, type(None)))
        verify_type("tags_remove", tags_remove, (dict, type(None)))

        verify_dict_type("configs", configs, (float, bool, int, str, datetime, list, set, tuple))
        verify_dict_type("tags_add", tags_add, (list, set, tuple))
        verify_dict_type("tags_remove", tags_remove, (list, set, tuple))

        # Don't log anything after we've been stopped. This allows continuing the training script
        # after a non-recoverable error happened. Note we don't to use self._lock in this check,
        # to keep the common path faster, because the benefit of locking here is minimal.
        if self._is_closing:
            return

        if self._attr_store is None:
            logger.debug("Run is in mode that doesn't support logging.")
            return

        self._attr_store.log(
            timestamp=timestamp,
            configs=configs,
            metrics=metrics,
            tags_add=tags_add,
            tags_remove=tags_remove,
        )

    def _wait(
        self,
        phrase: str,
        sleep_time: float,
        wait_seq: Optional[SharedInt],
        timeout: Optional[float] = None,
        verbose: bool = True,
    ) -> None:
        if verbose:
            logger.info(f"Waiting for all operations to be {phrase}")

        if timeout is None and verbose:
            logger.warning("No timeout specified. Waiting indefinitely")

        begin_time = time.time()
        wait_time = min(sleep_time, timeout) if timeout is not None else sleep_time
        last_print_timestamp: Optional[float] = None

        while True:
            try:
                with self._lock:
                    if self._sync_process is None or not self._sync_process.is_alive():
                        return  # No need to wait if the sync process is not running

                    assert wait_seq is not None
                    assert self._sequence_tracker is not None

                    # Handle the case where we get notified on `wait_seq` before we actually wait.
                    # Otherwise, we would unnecessarily block, waiting on a notify_all() that never happens.
                    if wait_seq.value >= self._sequence_tracker.last_sequence_id:
                        break

                with wait_seq:
                    wait_seq.wait(timeout=wait_time)
                    value = wait_seq.value

                last_queued_sequence_id = self._sequence_tracker.last_sequence_id

                if value == -1:
                    if self._sequence_tracker.last_sequence_id != -1:
                        last_print_timestamp = print_message(
                            f"Waiting. No operations were {phrase} yet. Operations to sync: %s",
                            self._sequence_tracker.last_sequence_id + 1,
                            last_print=last_print_timestamp,
                            verbose=verbose,
                        )
                    else:
                        last_print_timestamp = print_message(
                            f"Waiting. No operations were {phrase} yet",
                            last_print=last_print_timestamp,
                            verbose=verbose,
                        )
                elif value < last_queued_sequence_id:
                    last_print_timestamp = print_message(
                        f"Waiting for remaining %d operation(s) to be {phrase}",
                        last_queued_sequence_id - value + 1,
                        last_print=last_print_timestamp,
                        verbose=verbose,
                    )
                else:
                    # Reaching the last queued sequence ID means that all operations were submitted
                    if value >= last_queued_sequence_id or (timeout is not None and time.time() - begin_time > timeout):
                        break
            except KeyboardInterrupt:
                if verbose:
                    logger.warning("Waiting interrupted by user")
                return

        if verbose:
            logger.info(f"All operations were {phrase}")

    def wait_for_submission(self, timeout: Optional[float] = None, verbose: bool = True) -> None:
        """
        Waits until all metadata is submitted to Neptune for processing.

        When submitted, the data is not yet saved in Neptune until fully processed.
        See wait_for_processing().

        Args:
            timeout (float, optional): In seconds, the maximum time to wait for submission.
            verbose (bool): If True (default), prints messages about the waiting process.
        """
        self._wait(
            phrase="submitted",
            sleep_time=MINIMAL_WAIT_FOR_PUT_SLEEP_TIME,
            wait_seq=self._last_queued_seq,
            timeout=timeout,
            verbose=verbose,
        )

    def wait_for_processing(self, timeout: Optional[float] = None, verbose: bool = True) -> None:
        """
        Waits until all metadata is processed by Neptune.

        Once the call is complete, the data is saved in Neptune.

        Args:
            timeout (float, optional): In seconds, the maximum time to wait for processing.
            verbose (bool): If True (default), prints messages about the waiting process.
        """
        self._wait(
            phrase="processed",
            sleep_time=MINIMAL_WAIT_FOR_ACK_SLEEP_TIME,
            wait_seq=self._last_ack_seq,
            timeout=timeout,
            verbose=verbose,
        )


def print_message(msg: str, *args: Any, last_print: Optional[float] = None, verbose: bool = True) -> Optional[float]:
    current_time = time.time()

    if verbose and (last_print is None or current_time - last_print > STOP_MESSAGE_FREQUENCY):
        logger.info(msg, *args)
        return current_time

    return last_print


def _resolve_run_db_path(project: str, run_id: str, user_provided_log_dir: Optional[Union[str, Path]]) -> Path:
    sanitized_project = re.sub(r"[\\/]", "_", project)
    sanitized_run_id = re.sub(r"[\\/]", "_", run_id)
    timestamp_ns = int(time.time() * 1e9)
    directory = Path(os.getcwd()) / ".neptune" if user_provided_log_dir is None else Path(user_provided_log_dir)

    return (directory / f"{sanitized_project}_{sanitized_run_id}_{timestamp_ns}.sqlite3").absolute()
