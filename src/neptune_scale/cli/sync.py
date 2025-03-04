#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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
#

__all__ = ["sync_all"]

from datetime import datetime
from pathlib import Path
from typing import Optional, Generator

import backoff
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import UpdateRunSnapshots
from neptune_api.proto.neptune_pb.ingest.v1.pub.client_pb2 import SubmitResponse

from neptune_scale.exceptions import NeptuneRetryableError, NeptuneUnauthorizedError, NeptuneConnectionLostError, \
    NeptuneTooManyRequestsResponseError, NeptuneInternalServerError, NeptuneUnexpectedResponseError, \
    NeptuneSynchronizationStopped, NeptuneUnexpectedError
from neptune_scale.net.api_client import with_api_errors_handling, backend_factory, ApiClient
from neptune_scale.sync.operations_repository import OperationsRepository, SequenceId, Operation, OperationType
from neptune_scale.sync.parameters import MAX_REQUEST_RETRY_SECONDS
from neptune_scale.util import get_logger


logger = get_logger()


def sync_all(
    run_log_file: Path,
    api_token: str,
    sync_no_parent: bool,
) -> None:
    repository = OperationsRepository(db_path=run_log_file)

    runner = SyncRunner(api_token=api_token, operations_repository=repository)

    runner.work()


class SyncRunner:
    def __init__(
        self,
        api_token: str,
        operations_repository: OperationsRepository,
    ) -> None:
        self._api_token: str = api_token
        self._operations_repository: OperationsRepository = operations_repository

    def work(self) -> None:
        backend = backend_factory(api_token=self._api_token, mode="async")
        metadata = self._operations_repository.get_metadata()

        try:
            generator = self._stream_operations(metadata.run_id, metadata.project)
            while (operation := next(generator, None)) is not None:
                run_operation, sequence_id, timestamp = operation

                try:
                    logger.debug("Submitting operation #%d with size of %d bytes", sequence_id, len(""))
                    request_ids: Optional[SubmitResponse] = self._submit(
                        backend=backend, family=metadata.run_id, operation=run_operation
                    )

                    if request_ids is None or not request_ids.request_ids:
                        raise NeptuneUnexpectedError("Server response is empty")

                    last_request_id = request_ids.request_ids[-1]

                    logger.debug("Operation #%d submitted as %s", sequence_id, last_request_id)
                    # self._status_tracking_queue.put(
                    #     StatusTrackingElement(sequence_id=sequence_id, request_id=last_request_id, timestamp=timestamp)
                    # )

                    self._operations_repository.delete_operations(up_to_seq_id=sequence_id)
                except NeptuneRetryableError as e:
                    # self._errors_queue.put(e)
                    # Sleep before retry
                    pass

        except Exception as e:
            raise NeptuneSynchronizationStopped() from e


    @backoff.on_exception(backoff.expo, NeptuneRetryableError, max_time=MAX_REQUEST_RETRY_SECONDS)
    @with_api_errors_handling
    def _submit(self, backend: ApiClient, family: str, operation: RunOperation) -> Optional[SubmitResponse]:
        response = backend.submit(operation=operation, family=family)

        status_code = response.status_code
        if status_code != 200:
            self._raise_exception(status_code)

        return response.parsed

    @staticmethod
    def _raise_exception(status_code: int) -> None:
        logger.error("HTTP response error: %s", status_code)
        if status_code == 403:
            raise NeptuneUnauthorizedError()
        elif status_code == 408:
            raise NeptuneConnectionLostError()
        elif status_code == 429:
            raise NeptuneTooManyRequestsResponseError()
        elif status_code // 100 == 5:
            raise NeptuneInternalServerError()
        else:
            raise NeptuneUnexpectedResponseError()

    def _stream_operations(
        self,
        run_id: str,
        project: str,
        max_batch_size: int = 15 * 1024 * 1024,
    ) -> Generator[tuple[RunOperation, SequenceId, datetime], None, None]:
        while operations := self._operations_repository.get_operations(up_to_bytes=max_batch_size):
            if operations[0].operation_type == OperationType.CREATE_RUN:
                create_run = operations.pop(0)
                operation = RunOperation(project=project, run_id=run_id, create=create_run.operation)  # type: ignore
                yield operation, create_run.sequence_id, create_run.ts

                if not operations:
                    continue

            data, sequence_id, timestamp = self._merge_operations(operations)
            operation = RunOperation(project=project, run_id=run_id, update_batch=data)  # type: ignore
            yield operation, sequence_id, timestamp

    @staticmethod
    def _merge_operations(operations: list[Operation]) -> tuple[UpdateRunSnapshots, SequenceId, datetime]:
        snapshot_batch = UpdateRunSnapshots(snapshots=[op.operation for op in operations])  # type: ignore
        return snapshot_batch, operations[-1].sequence_id, operations[-1].ts
