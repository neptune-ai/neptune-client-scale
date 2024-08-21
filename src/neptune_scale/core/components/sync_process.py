from __future__ import annotations

__all__ = ("SyncProcess",)

import multiprocessing
import queue
from multiprocessing import (
    Process,
    Queue,
)
from multiprocessing.sharedctypes import Synchronized
from multiprocessing.synchronize import Condition
from typing import (
    Any,
    Callable,
    Literal,
    Optional,
)

import backoff
import httpx
from neptune_api.errors import (
    InvalidApiTokenException,
    UnableToDeserializeApiKeyError,
    UnableToExchangeApiKeyError,
    UnableToRefreshTokenError,
    UnexpectedStatus,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.client_pb2 import RequestId
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation
from neptune_api.types import Response

from neptune_scale.api.api_client import (
    ApiClient,
    HostedApiClient,
    MockedApiClient,
)
from neptune_scale.core.components.abstract import (
    Resource,
    WithResources,
)
from neptune_scale.core.components.daemon import Daemon
from neptune_scale.core.components.errors_tracking import ErrorsQueue
from neptune_scale.core.components.queue_element import QueueElement
from neptune_scale.core.logger import logger
from neptune_scale.exceptions import (
    NeptuneConnectionLostError,
    NeptuneInvalidCredentialsError,
    NeptuneOperationsQueueMaxSizeExceeded,
    NeptuneRetryableError,
    NeptuneUnableToAuthenticateError,
    NeptuneUnauthorizedError,
)
from neptune_scale.parameters import (
    EXTERNAL_TO_INTERNAL_THREAD_SLEEP_TIME,
    MAX_QUEUE_SIZE,
    OPERATION_TIMEOUT,
    SHUTDOWN_TIMEOUT,
    SYNC_THREAD_SLEEP_TIME,
)


def with_api_errors_handling(func: Callable[..., Any]) -> Callable[..., Any]:
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except (InvalidApiTokenException, UnableToDeserializeApiKeyError):
            raise NeptuneInvalidCredentialsError()
        except (UnableToRefreshTokenError, UnableToExchangeApiKeyError, UnexpectedStatus):
            raise NeptuneUnableToAuthenticateError()
        except (httpx.ConnectError, httpx.TimeoutException, httpx.RemoteProtocolError):
            raise NeptuneConnectionLostError()
        except Exception as e:
            raise e

    return wrapper


class SyncProcess(Process):
    def __init__(
        self,
        operations_queue: Queue,
        errors_queue: ErrorsQueue,
        api_token: str,
        family: str,
        mode: Literal["async", "disabled"],
        last_put_seq: Synchronized[int],
        last_put_seq_wait: Condition,
        max_queue_size: int = MAX_QUEUE_SIZE,
    ) -> None:
        super().__init__(name="SyncProcess")

        self._external_operations_queue: Queue[QueueElement] = operations_queue
        self._errors_queue: ErrorsQueue = errors_queue
        self._api_token: str = api_token
        self._family: str = family
        self._last_put_seq: Synchronized[int] = last_put_seq
        self._last_put_seq_wait: Condition = last_put_seq_wait
        self._max_queue_size: int = max_queue_size
        self._mode: Literal["async", "disabled"] = mode

    def run(self) -> None:
        logger.info("Data synchronization started")
        worker = SyncProcessWorker(
            family=self._family,
            api_token=self._api_token,
            errors_queue=self._errors_queue,
            external_operations_queue=self._external_operations_queue,
            last_put_seq=self._last_put_seq,
            last_put_seq_wait=self._last_put_seq_wait,
            max_queue_size=self._max_queue_size,
            mode=self._mode,
        )
        worker.start()
        try:
            worker.join()
        except KeyboardInterrupt:
            worker.interrupt()
            worker.wake_up()
            worker.join(timeout=SHUTDOWN_TIMEOUT)
            worker.close()


class SyncProcessWorker(WithResources):
    def __init__(
        self,
        *,
        api_token: str,
        family: str,
        errors_queue: ErrorsQueue,
        external_operations_queue: multiprocessing.Queue[QueueElement],
        last_put_seq: Synchronized[int],
        mode: Literal["async", "disabled"],
        last_put_seq_wait: Condition,
        max_queue_size: int = MAX_QUEUE_SIZE,
    ) -> None:
        self._errors_queue = errors_queue

        self._internal_operations_queue: queue.Queue[QueueElement] = queue.Queue(maxsize=max_queue_size)
        self._sync_thread = SyncThread(
            api_token=api_token,
            operations_queue=self._internal_operations_queue,
            errors_queue=self._errors_queue,
            family=family,
            last_put_seq=last_put_seq,
            last_put_seq_wait=last_put_seq_wait,
            mode=mode,
        )
        self._external_to_internal_thread = ExternalToInternalOperationsThread(
            external=external_operations_queue,
            internal=self._internal_operations_queue,
            errors_queue=self._errors_queue,
        )

    @property
    def threads(self) -> tuple[Daemon, ...]:
        return self._external_to_internal_thread, self._sync_thread

    @property
    def resources(self) -> tuple[Resource, ...]:
        return self._external_to_internal_thread, self._sync_thread

    def interrupt(self) -> None:
        for thread in self.threads:
            thread.interrupt()

    def wake_up(self) -> None:
        for thread in self.threads:
            thread.wake_up()

    def start(self) -> None:
        for thread in self.threads:
            thread.start()

    def join(self, timeout: Optional[int] = None) -> None:
        for thread in self.threads:
            thread.join(timeout=timeout)


class ExternalToInternalOperationsThread(Daemon, Resource):
    def __init__(
        self,
        external: multiprocessing.Queue[QueueElement],
        internal: queue.Queue[QueueElement],
        errors_queue: ErrorsQueue,
    ) -> None:
        super().__init__(name="ExternalToInternalOperationsThread", sleep_time=EXTERNAL_TO_INTERNAL_THREAD_SLEEP_TIME)

        self._external: multiprocessing.Queue[QueueElement] = external
        self._internal: queue.Queue[QueueElement] = internal
        self._errors_queue: ErrorsQueue = errors_queue
        self._latest_unprocessed: Optional[QueueElement] = None

    def get_next(self) -> Optional[QueueElement]:
        if self._latest_unprocessed is not None:
            return self._latest_unprocessed

        try:
            return self._external.get_nowait()
        except queue.Empty:
            return None

    def work(self) -> None:
        while (operation := self.get_next()) is not None:
            logger.debug("Copying operation #%d: %s", operation.sequence_id, operation)

            self._latest_unprocessed = operation
            try:
                self._internal.put_nowait(operation)
                self._latest_unprocessed = None
            except queue.Full:
                self._errors_queue.put(NeptuneOperationsQueueMaxSizeExceeded(max_size=self._internal.maxsize))
            except Exception as e:
                self._errors_queue.put(e)


def raise_for_status(response: Response[RequestId]) -> None:
    if response.status_code == 403:
        raise NeptuneUnauthorizedError()
    if response.status_code != 200:
        raise RuntimeError(f"Unexpected status code: {response.status_code}")


def _ensure_backend_initialized(api_token: str, mode: Literal["async", "disabled"]) -> ApiClient:
    if mode == "disabled":
        return MockedApiClient()
    return HostedApiClient(api_token=api_token)


class SyncThread(Daemon, WithResources):
    def __init__(
        self,
        api_token: str,
        operations_queue: queue.Queue[QueueElement],
        errors_queue: ErrorsQueue,
        family: str,
        last_put_seq: Synchronized[int],
        last_put_seq_wait: Condition,
        mode: Literal["async", "disabled"],
    ) -> None:
        super().__init__(name="SyncThread", sleep_time=SYNC_THREAD_SLEEP_TIME)

        self._api_token: str = api_token
        self._operations_queue: queue.Queue[QueueElement] = operations_queue
        self._errors_queue: ErrorsQueue = errors_queue
        self._backend: Optional[ApiClient] = None
        self._family: str = family
        self._last_put_seq: Synchronized[int] = last_put_seq
        self._last_put_seq_wait: Condition = last_put_seq_wait
        self._mode: Literal["async", "disabled"] = mode

        self._latest_unprocessed: Optional[QueueElement] = None

    def get_next(self) -> Optional[QueueElement]:
        if self._latest_unprocessed is not None:
            return self._latest_unprocessed

        try:
            return self._operations_queue.get_nowait()
        except queue.Empty:
            return None

    @property
    def resources(self) -> tuple[Resource, ...]:
        if self._backend is not None:
            return (self._backend,)
        return ()

    @backoff.on_exception(backoff.expo, NeptuneConnectionLostError, max_time=OPERATION_TIMEOUT)
    @with_api_errors_handling
    def submit(self, *, operation: RunOperation) -> None:
        if self._backend is None:
            self._backend = _ensure_backend_initialized(api_token=self._api_token, mode=self._mode)
        response = self._backend.submit(operation=operation, family=self._family)
        logger.debug("Server response:", response)
        raise_for_status(response)

    def work(self) -> None:
        while (operation := self.get_next()) is not None:
            self._latest_unprocessed = operation
            sequence_id, timestamp, data = operation

            try:
                run_operation = RunOperation()
                run_operation.ParseFromString(data)
                self.submit(operation=run_operation)
            except NeptuneRetryableError as e:
                self._errors_queue.put(e)
                continue
            except Exception as e:
                self._errors_queue.put(e)
                self.interrupt()
                self._last_put_seq_wait.notify_all()
                break

            self._latest_unprocessed = None

            # Update Last PUT sequence id and notify threads in the main process
            with self._last_put_seq_wait:
                self._last_put_seq.value = sequence_id
                self._last_put_seq_wait.notify_all()
