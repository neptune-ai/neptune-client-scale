from __future__ import annotations

import queue
from functools import partial
from typing import (
    Optional,
    cast,
)

from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale.exceptions import (
    NeptuneOperationsQueueMaxSizeExceeded,
    NeptuneSynchronizationStopped,
)
from neptune_scale.sync.aggregating_queue import AggregatingQueue
from neptune_scale.sync.errors_tracking import ErrorsQueue
from neptune_scale.sync.file_upload import FileUploader
from neptune_scale.sync.operations_queue import OperationsQueue
from neptune_scale.sync.parameters import INTERNAL_QUEUE_FEEDER_THREAD_SLEEP_TIME
from neptune_scale.sync.queue_element import (
    OperationMessage,
    OperationType,
    SingleOperation,
    UploadFile,
)
from neptune_scale.util import (
    Daemon,
    get_logger,
)
from neptune_scale.util.abstract import Resource

logger = get_logger()


class OperationDispatcherThread(Daemon, Resource):
    """Retrieves messages from the operations queue that is fed by the main process. Dispatches messages based on
    their type:
     * SingleOperation: common logging operations - push to the aggregating queue
     * UploadFileOperation: push to file upload queue
    """

    def __init__(
        self,
        input_queue: OperationsQueue,
        operations_queue: AggregatingQueue,
        errors_queue: ErrorsQueue,
        file_uploader: FileUploader,
    ) -> None:
        super().__init__(name="OperationDispatcherThread", sleep_time=INTERNAL_QUEUE_FEEDER_THREAD_SLEEP_TIME)

        self._input_queue = input_queue
        self._operations_queue: AggregatingQueue = operations_queue
        self._errors_queue: ErrorsQueue = errors_queue
        self._file_uploader: FileUploader = file_uploader

        self._latest_unprocessed: Optional[OperationMessage] = None

    def get_next(self) -> Optional[OperationMessage]:
        if self._latest_unprocessed is not None:
            return self._latest_unprocessed

        try:
            self._latest_unprocessed = self._input_queue.get(timeout=INTERNAL_QUEUE_FEEDER_THREAD_SLEEP_TIME)
            return self._latest_unprocessed
        except queue.Empty:
            return None

    def commit(self) -> None:
        self._latest_unprocessed = None

    def work(self) -> None:
        try:
            while not self._is_interrupted():
                message = self.get_next()
                if message is None:
                    continue

                if not self.dispatch(message):
                    break
        except Exception as e:
            self._errors_queue.put(e)
            self.interrupt()
            raise NeptuneSynchronizationStopped() from e

    def dispatch(self, message: OperationMessage) -> bool:
        try:
            if message.type == OperationType.SINGLE_OPERATION:
                self._operations_queue.put_nowait(cast(SingleOperation, message.operation))
            elif message.type == OperationType.UPLOAD_FILE:
                op = cast(UploadFile, message.operation)
                self._file_uploader.start_upload(
                    finalizer=partial(self._finalize_file_upload),
                    attribute_path=op.attribute_path,
                    local_path=op.local_path,
                    target_path=op.target_path,
                    target_basename=op.target_basename,
                )

            self.commit()
            return True
        except queue.Full:
            logger.debug(
                "Operations queue is full (%d elements), waiting for free space", self._operations_queue.maxsize
            )
            self._errors_queue.put(NeptuneOperationsQueueMaxSizeExceeded(max_size=self._operations_queue.maxsize))
            return False

    def _finalize_file_upload(self, path: str, error: Optional[Exception]) -> None:
        if error:
            self._errors_queue.put(error)
            return

        op = RunOperation()
        # TODO: Fill it out once we have established the protocol with the backend
        self._input_queue.enqueue_run_op(operation=op, size=op.ByteSize())
