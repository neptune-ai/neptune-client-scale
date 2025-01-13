import queue
import time
from dataclasses import dataclass
from typing import Any
from unittest.mock import Mock

import pytest
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
    UpdateRunSnapshot,
    Value,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.client_pb2 import SubmitResponse
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale.exceptions import NeptuneSynchronizationStopped
from neptune_scale.sync.queue_element import (
    BatchedOperations,
    SingleOperation,
)
from neptune_scale.sync.sync_process import SenderThread
from neptune_scale.util.shared_var import SharedInt


def response(request_ids: list[str], status_code: int = 200):
    body = SubmitResponse(request_ids=request_ids, request_id=request_ids[-1] if request_ids else None)
    content = body.SerializeToString()
    return Mock(status_code=status_code, content=content, parsed=body)


def single_operation(update: UpdateRunSnapshot, sequence_id):
    operation = RunOperation(update=update)
    return SingleOperation(
        sequence_id=sequence_id,
        timestamp=time.process_time(),
        operation=operation.SerializeToString(),
        is_batchable=True,
        metadata_size=update.ByteSize(),
    )


@dataclass
class MockedSender:
    operations_queue: Any
    status_tracking_queue: Any
    errors_queue: Any
    last_queue_seq: Any
    backend: Any
    sender_thread: Any


@pytest.fixture
def sender() -> MockedSender:
    operations_queue = Mock()
    status_tracking_queue = Mock()
    errors_queue = Mock()
    last_queue_seq = SharedInt(initial_value=0)
    backend = Mock()
    sender_thread = SenderThread(
        api_token="",
        family="",
        operations_queue=operations_queue,
        status_tracking_queue=status_tracking_queue,
        errors_queue=errors_queue,
        last_queued_seq=last_queue_seq,
        mode="disabled",
    )
    sender_thread._backend = backend

    return MockedSender(operations_queue, status_tracking_queue, errors_queue, last_queue_seq, backend, sender_thread)


def test_sender_thread_work_finishes_when_queue_empty(sender):
    # given
    sender.operations_queue.get.side_effect = queue.Empty

    # when
    sender.sender_thread.work()

    # then
    assert True


def test_sender_thread_processes_single_element(sender):
    # given
    update = UpdateRunSnapshot(assign={"key": Value(string="a")})
    element = single_operation(update, sequence_id=2)
    sender.operations_queue.get.side_effect = [
        BatchedOperations(sequence_id=element.sequence_id, timestamp=element.timestamp, operation=element.operation),
        queue.Empty,
    ]

    # and
    sender.backend.submit.side_effect = [response(["1"])]

    # when
    sender.sender_thread.work()

    # then
    assert sender.backend.submit.call_count == 1


def test_sender_thread_processes_element_on_single_retryable_error(sender):
    # given
    update = UpdateRunSnapshot(assign={"key": Value(string="a")})
    element = single_operation(update, sequence_id=2)
    sender.operations_queue.get.side_effect = [
        BatchedOperations(sequence_id=element.sequence_id, timestamp=element.timestamp, operation=element.operation),
        queue.Empty,
    ]

    # and
    sender.backend.submit.side_effect = [
        response([], status_code=503),
        response(["a"], status_code=200),
    ]

    # when
    sender.sender_thread.work()

    # then
    assert sender.backend.submit.call_count == 2


def test_sender_thread_fails_on_regular_error(sender):
    # given
    update = UpdateRunSnapshot(assign={"key": Value(string="a")})
    element = single_operation(update, sequence_id=2)
    sender.operations_queue.get.side_effect = [
        BatchedOperations(sequence_id=element.sequence_id, timestamp=element.timestamp, operation=element.operation),
        queue.Empty,
    ]

    # and
    sender.backend.submit.side_effect = [
        response([], status_code=200),
    ]

    # when
    with pytest.raises(NeptuneSynchronizationStopped):
        sender.sender_thread.work()

    # then should throw NeptuneInternalServerError
    sender.errors_queue.put.assert_called_once()


def test_sender_thread_processes_element_on_429_and_408_http_statuses(sender):
    # given
    update = UpdateRunSnapshot(assign={"key": Value(string="a")})
    element = single_operation(update, sequence_id=2)
    sender.operations_queue.get.side_effect = [
        BatchedOperations(sequence_id=element.sequence_id, timestamp=element.timestamp, operation=element.operation),
        queue.Empty,
    ]

    # and
    sender.backend.submit.side_effect = [
        response([], status_code=408),
        response([], status_code=429),
        response(["a"], status_code=200),
    ]

    # when
    sender.sender_thread.work()

    # then
    assert sender.backend.submit.call_count == 3
