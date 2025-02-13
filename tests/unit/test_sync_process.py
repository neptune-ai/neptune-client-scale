import queue
import time
from unittest.mock import Mock

import neptune_api.proto.neptune_pb.ingest.v1.ingest_pb2 as ingest_pb2
import pytest
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
    UpdateRunSnapshot,
    Value,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.client_pb2 import SubmitResponse
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale import NeptuneScaleWarning
from neptune_scale.exceptions import (
    NeptuneScaleError,
    NeptuneSynchronizationStopped, NeptuneUnexpectedError,
)
from neptune_scale.sync.queue_element import (
    BatchedOperations,
    SingleOperation,
)
from neptune_scale.sync.sync_process import (
    SenderThread,
    code_to_exception,
)
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
        batch_key=None,
    )


def test_sender_thread_work_finishes_when_queue_empty():
    # given
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

    # and
    operations_queue.get.side_effect = queue.Empty

    # when
    sender_thread.work()

    # then
    assert True


def test_sender_thread_processes_single_element():
    # given
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

    # and
    update = UpdateRunSnapshot(assign={"key": Value(string="a")})
    element = single_operation(update, sequence_id=2)
    operations_queue.get.side_effect = [
        BatchedOperations(sequence_id=element.sequence_id, timestamp=element.timestamp, operation=element.operation),
        queue.Empty,
    ]

    # and
    backend.submit.side_effect = [response(["1"])]

    # when
    sender_thread.work()

    # then
    assert backend.submit.call_count == 1


def test_sender_thread_processes_element_on_single_retryable_error():
    # given
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

    # and
    update = UpdateRunSnapshot(assign={"key": Value(string="a")})
    element = single_operation(update, sequence_id=2)
    operations_queue.get.side_effect = [
        BatchedOperations(sequence_id=element.sequence_id, timestamp=element.timestamp, operation=element.operation),
        queue.Empty,
    ]

    # and
    backend.submit.side_effect = [
        response([], status_code=503),
        response(["a"], status_code=200),
    ]

    # when
    sender_thread.work()

    # then
    assert backend.submit.call_count == 2


def test_sender_thread_fails_on_regular_error():
    # given
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

    # and
    update = UpdateRunSnapshot(assign={"key": Value(string="a")})
    element = single_operation(update, sequence_id=2)
    operations_queue.get.side_effect = [
        BatchedOperations(sequence_id=element.sequence_id, timestamp=element.timestamp, operation=element.operation),
        queue.Empty,
    ]

    # and
    backend.submit.side_effect = [
        response([], status_code=200),
    ]

    # when
    with pytest.raises(NeptuneSynchronizationStopped):
        sender_thread.work()

    # then should throw NeptuneInternalServerError
    errors_queue.put.assert_called_once()


def test_sender_thread_processes_element_on_429_and_408_http_statuses():
    # given
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

    # and
    update = UpdateRunSnapshot(assign={"key": Value(string="a")})
    element = single_operation(update, sequence_id=2)
    operations_queue.get.side_effect = [
        BatchedOperations(sequence_id=element.sequence_id, timestamp=element.timestamp, operation=element.operation),
        queue.Empty,
    ]

    # and
    backend.submit.side_effect = [
        response([], status_code=408),
        response([], status_code=429),
        response(["a"], status_code=200),
    ]

    # when
    sender_thread.work()

    # then
    assert backend.submit.call_count == 3


@pytest.mark.parametrize(
    "code",
    ingest_pb2.IngestCode.DESCRIPTOR.values_by_number.keys(),
    ids=ingest_pb2.IngestCode.DESCRIPTOR.values_by_name.keys(),
)
def test_code_to_exception(code):
    exception = code_to_exception(code)
    assert isinstance(exception, NeptuneScaleError) or isinstance(exception, NeptuneScaleWarning)


def test_unknown_code_to_exception():
    code = 100_000 - 1
    exception = code_to_exception(code)
    assert isinstance(exception, NeptuneUnexpectedError)
    assert f"Unexpected ingestion error code: {code}" in str(exception)
