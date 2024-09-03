"""
Python package
"""

from __future__ import annotations

__all__ = ["Run"]

import atexit
import multiprocessing
import os
import platform
import signal
import threading
import time
from contextlib import AbstractContextManager
from datetime import datetime
from multiprocessing.sharedctypes import Synchronized
from multiprocessing.synchronize import Condition as ConditionT
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Set,
    Union,
)

from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import ForkPoint
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import Run as CreateRun
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale.core.components.abstract import (
    Resource,
    WithResources,
)
from neptune_scale.core.components.errors_tracking import (
    ErrorsMonitor,
    ErrorsQueue,
)
from neptune_scale.core.components.operations_queue import OperationsQueue
from neptune_scale.core.components.sync_process import SyncProcess
from neptune_scale.core.logger import logger
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
    MINIMAL_WAIT_FOR_ACK_SLEEP_TIME,
    MINIMAL_WAIT_FOR_PUT_SLEEP_TIME,
    STOP_MESSAGE_FREQUENCY,
)


class Run(WithResources, AbstractContextManager):
    """
    Representation of experiment tracking metadata logged with neptune.ai.

    Methods:
        close(): Synchronizes all remaining data and closes the connection to Neptune.
        log(): Logs the specified metadata to Neptune.
        wait_for_submission(): Waits until all metadata is submitted to Neptune for processing.
        wait_for_processing(): Waits until all metadata is processed by Neptune.
    """

    def __init__(
        self,
        *,
        family: str,
        run_id: str,
        project: Optional[str] = None,
        api_token: Optional[str] = None,
        resume: bool = False,
        mode: Literal["async", "disabled"] = "async",
        as_experiment: Optional[str] = None,
        creation_time: Optional[datetime] = None,
        from_run_id: Optional[str] = None,
        from_step: Optional[Union[int, float]] = None,
        max_queue_size: int = MAX_QUEUE_SIZE,
        on_queue_full_callback: Optional[Callable[[BaseException, Optional[float]], None]] = None,
        on_network_error_callback: Optional[Callable[[BaseException, Optional[float]], None]] = None,
        on_error_callback: Optional[Callable[[BaseException, Optional[float]], None]] = None,
        on_warning_callback: Optional[Callable[[BaseException, Optional[float]], None]] = None,
    ) -> None:
        """
        Initializes a Neptune run that logs model-building metadata.

        Args:
            family (str): Identifies related runs. All runs of the same lineage must have the same `family` value, that is,
                forking is only possible within the same family. Max length: 128 characters.
            run_id (str): Identifier of the run. Must be unique within the project. Max length: 128 characters.
            project (str): Name of the project where the metadata is logged, in the form `workspace-name/project-name`.
                If not provided, the value of the `NEPTUNE_PROJECT` environment variable is used.
            api_token (str): Your Neptune API token. If not provided, the value of the `NEPTUNE_API_TOKEN` environment
                variable is used.
            resume (bool): If `False` (default), creates a new run. To continue an existing run, set to `True` and pass
                the ID of an existing run to the `run_id` argument.
                To fork a run, use `from_run_id` and `from_step` instead.
            mode ("async" or "disabled"): Mode of operation. If set to "disabled", the run doesn't log any metadata.
            as_experiment (str): Name of the experiment to associate the run with.
            creation_time (datetime): Custom creation time of the run.
            from_run_id (str): If forking off an existing run, ID of the run to fork from.
            from_step (int): If forking off an existing run, step number to fork from.
            max_queue_size (int): Maximum number of operations allowed in the queue.
            on_queue_full_callback (Callable[[BaseException, Optional[float]], None]): Callback function triggered when
                the queue is full. The function takes two arguments:
                - Exception that made the queue full.
                - (Optional) Timestamp of the last time the exception was raised.
            on_network_error_callback: Callback function triggered when a network error occurs.
            on_error_callback: The default callback function triggered when an error occurs. Applies if an error
                wasn't caught by other callbacks.
            on_warning_callback: Callback function triggered when a warning occurs.

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
            with Run(
                family="aquarium",
                run_id="adventurous-barracuda",
                as_experiment="swim-further",
                from_run_id="likable-barracuda",
                from_step=102,
            ) as run:
                ...
            ```

            Continue a run:

            ```
            with Run(
                family="aquarium",
                run_id="likable-barracuda",  # run with this ID already exists
                resume=True,
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
        verify_type("max_queue_size_exceeded_callback", on_queue_full_callback, (Callable, type(None)))

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

        if max_queue_size < 1:
            raise ValueError("`max_queue_size` must be greater than 0.")

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
            lock=self._lock,
            max_size=max_queue_size,
        )
        self._errors_queue: ErrorsQueue = ErrorsQueue()
        self._errors_monitor = ErrorsMonitor(
            errors_queue=self._errors_queue,
            on_queue_full_callback=on_queue_full_callback,
            on_network_error_callback=on_network_error_callback,
            on_error_callback=on_error_callback,
            on_warning_callback=on_warning_callback,
        )

        self._last_put_seq: Synchronized[int] = multiprocessing.Value("i", -1)
        self._last_put_seq_wait: ConditionT = multiprocessing.Condition()

        self._last_ack_seq: Synchronized[int] = multiprocessing.Value("i", -1)
        self._last_ack_seq_wait: ConditionT = multiprocessing.Condition()

        self._sync_process = SyncProcess(
            project=self._project,
            family=self._family,
            operations_queue=self._operations_queue.queue,
            errors_queue=self._errors_queue,
            api_token=input_api_token,
            last_put_seq=self._last_put_seq,
            last_put_seq_wait=self._last_put_seq_wait,
            last_ack_seq=self._last_ack_seq,
            last_ack_seq_wait=self._last_ack_seq_wait,
            max_queue_size=max_queue_size,
            mode=mode,
        )

        self._errors_monitor.start()
        with self._lock:
            self._sync_process.start()

        self._exit_func: Optional[Callable[[], None]] = atexit.register(self._close)

        if platform.system() != "Windows":
            signal.signal(signal.SIGCHLD, self._handle_signal)

        if not resume:
            self._create_run(
                creation_time=datetime.now() if creation_time is None else creation_time,
                as_experiment=as_experiment,
                from_run_id=from_run_id,
                from_step=from_step,
            )
            self.wait_for_processing(verbose=False)

    def _handle_signal(self, signum: int, frame: Any) -> None:
        logger.debug(f"Received signal {signum}. Closing.")
        self.close()

    @property
    def resources(self) -> tuple[Resource, ...]:
        return (
            self._errors_queue,
            self._operations_queue,
            self._errors_monitor,
        )

    def _close(self) -> None:
        with self._lock:
            if self._sync_process.is_alive():
                self.wait_for_processing()
                self._sync_process.terminate()
                self._sync_process.join()

        self._errors_monitor.interrupt()
        self._errors_monitor.join()

        super().close()

    def close(self) -> None:
        """
        Stops the connection to Neptune and synchronizes all data.
        """
        if self._exit_func is not None:
            atexit.unregister(self._exit_func)
            self._exit_func = None
        self._close()

    def _create_run(
        self,
        creation_time: datetime,
        as_experiment: Optional[str],
        from_run_id: Optional[str],
        from_step: Optional[Union[int, float]],
    ) -> None:
        fork_point: Optional[ForkPoint] = None
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
        self._operations_queue.enqueue(operation=operation)

    def log(
        self,
        step: Optional[Union[float, int]] = None,
        timestamp: Optional[datetime] = None,
        fields: Optional[Dict[str, Union[float, bool, int, str, datetime, list, set]]] = None,
        metrics: Optional[Dict[str, float]] = None,
        add_tags: Optional[Dict[str, Union[List[str], Set[str]]]] = None,
        remove_tags: Optional[Dict[str, Union[List[str], Set[str]]]] = None,
    ) -> None:
        """
        Logs the specified metadata to a Neptune run.

        You can log metrics, tags, and configurations. Pass the metadata as a dictionary {key: value} with

        - key: path to where the metadata should be stored in the run.
        - value: the piece of metadata to log.

        For example, log(fields={"parameters/learning_rate": 0.001})
        In the field path, each forward slash "/" nests the field under a namespace.
        Use namespaces to structure the metadata into meaningful categories.

        Args:
            step: Index of the log entry. Must be increasing.
                If None, the highest of the already logged indexes is used.
            timestamp: Time of logging the metadata.
            fields: Dictionary of configs or other values to log. Independent of the step value.
                Available types: float, integer, Boolean, string, and datetime.
                To log multiple values at once, pass multiple dictionaries.
            metrics: Dictionary of metrics to log. Each metric value is associated with a step.
                To log multiple metrics at once, pass multiple dictionaries.
                Each metric is represented as a series of float values in the run.
            add_tags: Dictionary of tags to add to the run, as a list of strings. Independent of the step value.
            remove_tags: Dictionary of tags to remove from the run, as a list of strings. Independent of the step value.

        Examples:
            ```
            >>> from neptune_scale import Run
            >>> with Run(...) as run:
            ...     run.log(fields={"parameters/learning_rate": 0.001})
            ...     run.log(add_tags={"sys/tags": ["tag1", "tag2"]})
            ...     run.log(step=1, metrics={"loss": 0.11, "acc": 0.81})
            >>> run.log(step=2, metrics={"loss": 0.09, "acc": 0.82})
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
            self._operations_queue.enqueue(operation=operation)

    def _wait(
        self,
        phrase: str,
        sleep_time: float,
        wait_condition: ConditionT,
        external_value: Synchronized[int],
        timeout: Optional[float] = None,
        verbose: bool = True,
    ) -> None:
        if verbose:
            logger.info(f"Waiting for all operations to be {phrase}")

        if timeout is None and verbose:
            logger.warning("No timeout specified. Waiting indefinitely")

        begin_time = time.time()
        wait_time = min(sleep_time, timeout) if timeout is not None else sleep_time
        last_queued_sequence_id = self._operations_queue.last_sequence_id
        last_print_timestamp: Optional[float] = None

        while True:
            try:
                with self._lock:
                    if not self._sync_process.is_alive():
                        if verbose:
                            logger.warning("Sync process is not running")
                        return  # No need to wait if the sync process is not running

                with wait_condition:
                    wait_condition.wait(timeout=wait_time)
                    value = external_value.value

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
                else:
                    last_print_timestamp = print_message(
                        f"Waiting for remaining %d operation(s) to be {phrase}",
                        last_queued_sequence_id - value + 1,
                        last_print=last_print_timestamp,
                        verbose=verbose,
                    )

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
        Waits until all metadata is submitted to Neptune.

        Args:
            timeout (float, optional): In seconds, the maximum time to wait for submission.
            verbose (bool): If True (default), prints messages about the waiting process.
        """
        self._wait(
            phrase="submitted",
            sleep_time=MINIMAL_WAIT_FOR_PUT_SLEEP_TIME,
            wait_condition=self._last_put_seq_wait,
            external_value=self._last_put_seq,
            timeout=timeout,
            verbose=verbose,
        )

    def wait_for_processing(self, timeout: Optional[float] = None, verbose: bool = True) -> None:
        """
        Waits until all metadata is processed by Neptune.

        Args:
            timeout (float, optional): In seconds, the maximum time to wait for processing.
            verbose (bool): If True (default), prints messages about the waiting process.
        """
        self._wait(
            phrase="processed",
            sleep_time=MINIMAL_WAIT_FOR_ACK_SLEEP_TIME,
            wait_condition=self._last_ack_seq_wait,
            external_value=self._last_ack_seq,
            timeout=timeout,
            verbose=verbose,
        )


def print_message(msg: str, *args: Any, last_print: Optional[float] = None, verbose: bool = True) -> Optional[float]:
    current_time = time.time()

    if verbose and (last_print is None or current_time - last_print > STOP_MESSAGE_FREQUENCY):
        logger.info(msg, *args)
        return current_time

    return last_print
