#
# Copyright (c) 2025, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Python package
"""

from __future__ import annotations

__all__ = ["Run"]

import atexit
import math
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
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale.api.attribute import AttributeStore
from neptune_scale.api.validation import (
    verify_dict_type,
    verify_max_length,
    verify_non_empty,
    verify_project_qualified_name,
    verify_type,
)
from neptune_scale.exceptions import (
    NeptuneApiTokenNotProvided,
    NeptuneProjectNotProvided,
    NeptuneScaleError,
    NeptuneSynchronizationStopped,
)
from neptune_scale.net.serialization import (
    datetime_to_proto,
    make_step,
)
from neptune_scale.sync.errors_tracking import (
    ErrorsMonitor,
    ErrorsQueue,
    default_error_callback,
)
from neptune_scale.sync.lag_tracking import LagTracker
from neptune_scale.sync.operations_queue import OperationsQueue
from neptune_scale.sync.parameters import (
    MAX_EXPERIMENT_NAME_LENGTH,
    MAX_QUEUE_SIZE,
    MAX_RUN_ID_LENGTH,
    MINIMAL_WAIT_FOR_ACK_SLEEP_TIME,
    MINIMAL_WAIT_FOR_PUT_SLEEP_TIME,
    STOP_MESSAGE_FREQUENCY,
)
from neptune_scale.sync.sync_process import SyncProcess
from neptune_scale.types import RunCallback
from neptune_scale.util.abstract import (
    Resource,
    WithResources,
)
from neptune_scale.util.envs import (
    API_TOKEN_ENV_NAME,
    PROJECT_ENV_NAME,
)
from neptune_scale.util.logger import get_logger
from neptune_scale.util.process_link import ProcessLink
from neptune_scale.util.shared_var import (
    SharedFloat,
    SharedInt,
)

logger = get_logger()


class Run(WithResources, AbstractContextManager):
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
        mode: Literal["async", "disabled"] = "async",
        experiment_name: Optional[str] = None,
        creation_time: Optional[datetime] = None,
        fork_run_id: Optional[str] = None,
        fork_step: Optional[Union[int, float]] = None,
        max_queue_size: int = MAX_QUEUE_SIZE,
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
            mode: Mode of operation. If set to "disabled", the run doesn't log any metadata.
            experiment_name: If creating a run as an experiment, name (ID) of the experiment to be associated with the run.
            creation_time: Custom creation time of the run.
            fork_run_id: If forking from an existing run, ID of the run to fork from.
            fork_step: If forking from an existing run, step number to fork from.
            max_queue_size: Maximum number of operations in a queue.
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
        verify_type("max_queue_size", max_queue_size, int)
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

        if max_queue_size < 1:
            raise ValueError("`max_queue_size` must be greater than 0.")

        project = project or os.environ.get(PROJECT_ENV_NAME)
        if project:
            project = project.strip('"').strip("'")
        else:
            raise NeptuneProjectNotProvided()
        assert project is not None  # mypy
        input_project: str = project

        api_token = api_token or os.environ.get(API_TOKEN_ENV_NAME)
        if api_token is None:
            raise NeptuneApiTokenNotProvided()
        assert api_token is not None  # mypy
        input_api_token: str = api_token

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
        # This is used to signal that the close/termination operation is completed and block user code until it is so
        self._close_completed = threading.Event()
        # Thread that initially called _close()
        self._closing_thread: Optional[threading.Thread] = None
        # Mark that __init__() is finished, so that the error callback can act upon it (see _wrap_error_callback())
        self._init_completed = False

        self._project: str = input_project
        self._run_id: str = run_id

        self._lock = threading.RLock()
        self._operations_queue: OperationsQueue = OperationsQueue(
            lock=self._lock,
            max_size=max_queue_size,
        )

        self._attr_store: AttributeStore = AttributeStore(self._project, self._run_id, self._operations_queue)

        self._errors_queue: ErrorsQueue = ErrorsQueue()
        # Note that for the duration of __init__ we use a special error callback that
        # is guaranteed to terminate the run in case of an error.
        self._errors_monitor = ErrorsMonitor(
            errors_queue=self._errors_queue,
            on_queue_full_callback=on_queue_full_callback,
            on_network_error_callback=on_network_error_callback,
            on_error_callback=self._wrap_error_callback(on_error_callback),
            on_warning_callback=on_warning_callback,
        )

        self._last_queued_seq = SharedInt(-1)
        self._last_ack_seq = SharedInt(-1)
        self._last_ack_timestamp = SharedFloat(-1)

        self._process_link = ProcessLink()
        self._sync_process = SyncProcess(
            project=self._project,
            family=self._run_id,
            operations_queue=self._operations_queue.queue,
            errors_queue=self._errors_queue,
            process_link=self._process_link,
            api_token=input_api_token,
            last_queued_seq=self._last_queued_seq,
            last_ack_seq=self._last_ack_seq,
            last_ack_timestamp=self._last_ack_timestamp,
            max_queue_size=max_queue_size,
            mode=mode,
        )
        self._lag_tracker: Optional[LagTracker] = None
        if async_lag_threshold is not None and on_async_lag_callback is not None:
            self._lag_tracker = LagTracker(
                errors_queue=self._errors_queue,
                operations_queue=self._operations_queue,
                last_ack_timestamp=self._last_ack_timestamp,
                async_lag_threshold=async_lag_threshold,
                on_async_lag_callback=on_async_lag_callback,
            )
            self._lag_tracker.start()

        self._errors_monitor.start()
        with self._lock:
            self._sync_process.start()
            self._process_link.start(on_link_closed=self._on_child_link_closed)

        self._exit_func: Optional[Callable[[], None]] = atexit.register(self._close)

        if not resume:
            self._create_run(
                creation_time=datetime.now() if creation_time is None else creation_time,
                experiment_name=experiment_name,
                fork_run_id=fork_run_id,
                fork_step=fork_step,
            )

            # Wait in short periods to return from __init__ if run creation fails
            # and Run.terminate() is called in self._wrap_error_callback()
            while not self._is_closing:
                if self.wait_for_processing(verbose=False, timeout=1):
                    break

        with self._lock:
            self._init_completed = True

    def _wrap_error_callback(self, user_callback: Optional[RunCallback]) -> RunCallback:
        """Wrapp the provided user error callback so that we can handle errors during __init__()"""
        callback = user_callback or default_error_callback

        def wrapped_callback(error: BaseException, last_seen_at: Optional[float]) -> None:
            with self._lock:
                if not self._init_completed:
                    self.terminate()

            callback(error, last_seen_at)

        return wrapped_callback

    def _on_child_link_closed(self, _: ProcessLink) -> None:
        with self._lock:
            if not self._is_closing:
                logger.error("Child process closed unexpectedly. Terminating.")

        # Make sure all the error handling is done from a single thread - self._errors_monitor
        self._errors_queue.put(NeptuneSynchronizationStopped())

    @property
    def resources(self) -> tuple[Resource, ...]:
        if self._lag_tracker is not None:
            return (
                self._errors_queue,
                self._operations_queue,
                self._lag_tracker,
                self._errors_monitor,
            )
        return (
            self._errors_queue,
            self._operations_queue,
            self._errors_monitor,
        )

    def _close(self, *, wait: bool = True) -> None:
        with self._lock:
            was_closing = self._is_closing
            if not self._is_closing:
                self._is_closing = True
                self._closing_thread = threading.current_thread()

        if was_closing:
            logger.debug("Waiting for run to be closed from a different thread")
            # TODO: we should probably have a reasonable timeout here, same one as a default one in
            #  wait_for_processing(). Or just accept indefinite wait here in both cases.
            # if not self._close_completed.wait(timeout=...):
            if not self._close_completed.wait():
                raise NeptuneScaleError(reason="Run close operation timed out")
            return

        logger.debug(f"Run is closing, wait={wait}")

        if self._sync_process.is_alive():
            if wait:
                self.wait_for_processing()

            self._sync_process.terminate()
            self._sync_process.join()
            self._process_link.stop()

        if self._lag_tracker is not None:
            self._lag_tracker.interrupt()
            self._lag_tracker.wake_up()
            self._lag_tracker.join()

        self._errors_monitor.interrupt()

        # Don't call join() if being called from the error thread, as this will
        # result in a "cannot join current thread" exception.
        if threading.current_thread() != self._errors_monitor:
            self._errors_monitor.join()

        self._close_completed.set()

        super().close()

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

        if not self._is_closing:
            logger.info("Terminating Run.")

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

    def _create_run(
        self,
        creation_time: datetime,
        experiment_name: Optional[str],
        fork_run_id: Optional[str],
        fork_step: Optional[Union[int, float]],
    ) -> None:
        fork_point: Optional[ForkPoint] = None
        if fork_run_id is not None and fork_step is not None:
            fork_point = ForkPoint(
                parent_project=self._project, parent_run_id=fork_run_id, step=make_step(number=fork_step)
            )

        operation = RunOperation(
            project=self._project,
            run_id=self._run_id,
            create=CreateRun(
                family=self._run_id,
                fork_point=fork_point,
                experiment_id=experiment_name,
                creation_time=None if creation_time is None else datetime_to_proto(creation_time),
            ),
        )
        self._operations_queue.enqueue(operation=operation)

    def log_metrics(
        self,
        data: dict[str, Union[float, int]],
        step: Union[float, int],
        *,
        timestamp: Optional[datetime] = None,
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
            step: Index of the log entry. Must be increasing.
                Tip: Using float rather than int values can be useful, for example, when logging substeps in a batch.
            timestamp (optional): Time of logging the metadata. If not provided, the current time is used. If provided,
                and `timestamp.tzinfo` is not set, the time is assumed to be in the local timezone.


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
        self.log(step=step, timestamp=timestamp, metrics=data)

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
        self.log(configs=data)

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
        self.log(tags_add={name: tags})

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
        self.log(tags_remove={name: tags})

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
        See one of the following instead:

        - log_configs()
        - log_metrics()
        - add_tags()
        - remove_tags()
        """

        verify_type("step", step, (float, int, type(None)))
        verify_type("timestamp", timestamp, (datetime, type(None)))
        verify_type("configs", configs, (dict, type(None)))
        verify_type("metrics", metrics, (dict, type(None)))
        verify_type("tags_add", tags_add, (dict, type(None)))
        verify_type("tags_remove", tags_remove, (dict, type(None)))

        verify_dict_type("configs", configs, (float, bool, int, str, datetime, list, set, tuple))
        verify_dict_type("metrics", metrics, (float, int))
        verify_dict_type("tags_add", tags_add, (list, set, tuple))
        verify_dict_type("tags_remove", tags_remove, (list, set, tuple))

        # Don't log anything after we've been stopped. This allows continuing the training script
        # after a non-recoverable error happened. Note we don't to use self._lock in this check,
        # to keep the common path faster, because the benefit of locking here is minimal.
        if self._is_closing:
            return

        self._attr_store.log(
            step=step, timestamp=timestamp, configs=configs, metrics=metrics, tags_add=tags_add, tags_remove=tags_remove
        )

    def _wait(
        self,
        phrase: str,
        sleep_time: float,
        wait_seq: SharedInt,
        timeout: Optional[float] = None,
        verbose: bool = True,
    ) -> bool:
        if verbose:
            logger.info(f"Waiting for all operations to be {phrase}")

        if timeout is None:
            if verbose:
                logger.warning("No timeout specified. Waiting indefinitely")
            timeout = math.inf

        begin_time = time.monotonic()
        wait_time = min(sleep_time, timeout)
        last_print_timestamp: Optional[float] = None

        while True:
            try:
                with self._lock:
                    is_closing = self._is_closing

                    # Handle the case where we get notified on `wait_seq` before we actually wait.
                    # Otherwise, we would unnecessarily block, waiting on a notify_all() that never happens.
                    if wait_seq.value >= self._operations_queue.last_sequence_id:
                        return True

                if is_closing:
                    if threading.current_thread() != self._closing_thread:
                        if verbose:
                            logger.warning("Waiting interrupted by run termination")

                        self._close_completed.wait(wait_time)

                    return False

                with wait_seq:
                    wait_seq.wait(timeout=wait_time)
                    value = wait_seq.value

                last_queued_sequence_id = self._operations_queue.last_sequence_id

                if value == -1:
                    if self._operations_queue.last_sequence_id != -1:
                        last_print_timestamp = print_message(
                            f"Waiting. No operations were {phrase} yet. Operations to sync: %s",
                            self._operations_queue.last_sequence_id + 1,
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
                # Reaching the last queued sequence ID means that all operations were submitted
                elif value >= last_queued_sequence_id:
                    if verbose:
                        logger.info(f"All operations were {phrase}")
                    return True

                if time.monotonic() - begin_time > timeout:
                    return False
            except KeyboardInterrupt:
                if verbose:
                    logger.warning("Waiting interrupted by user")
                return False

        return False

    def wait_for_submission(self, timeout: Optional[float] = None, verbose: bool = True) -> bool:
        """
        Waits until all metadata is submitted to Neptune for processing.

        When submitted, the data is not yet saved in Neptune until fully processed.
        See wait_for_processing().

        Returns True if all currently queued operations were submitted, False if timeout was reached
        or Run is closing.

        Args:
            timeout (float, optional): In seconds, the maximum time to wait for submission.
            verbose (bool): If True (default), prints messages about the waiting process.
        """
        return self._wait(
            phrase="submitted",
            sleep_time=MINIMAL_WAIT_FOR_PUT_SLEEP_TIME,
            wait_seq=self._last_queued_seq,
            timeout=timeout,
            verbose=verbose,
        )

    def wait_for_processing(self, timeout: Optional[float] = None, verbose: bool = True) -> bool:
        """
        Waits until all metadata is processed by Neptune.

        Once the call is complete, the data is saved in Neptune.

        Returns True if all currently queued operations were processed, False if timeout was reached
        or Run is closing.

        Args:
            timeout (float, optional): In seconds, the maximum time to wait for processing.
            verbose (bool): If True (default), prints messages about the waiting process.
        """
        return self._wait(
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
