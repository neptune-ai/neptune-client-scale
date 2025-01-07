import queue
import time
from unittest.mock import Mock

import pytest
from neptune_api.proto.google_rpc.code_pb2 import Code
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
    UpdateRunSnapshot,
    Value,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.client_pb2 import (
    BulkRequestStatus,
    SubmitResponse,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation
from neptune_api.proto.neptune_pb.ingest.v1.pub.request_status_pb2 import RequestStatus

from neptune_scale.exceptions import NeptuneSynchronizationStopped
from neptune_scale.sync.queue_element import (
    BatchedOperations,
    SingleOperation,
)
from neptune_scale.sync.sync_process import (
    SenderThread,
    StatusTrackingElement,
    StatusTrackingThread,
)
from neptune_scale.util.shared_var import (
    SharedFloat,
    SharedInt,
)


def status_response(code_count_batch: list[dict[Code.ValueType, int]], status_code: int = 200):
    body = BulkRequestStatus(
        statuses=[
            RequestStatus(
                code_by_count=[RequestStatus.CodeByCount(code=code, count=count) for code, count in code_counts.items()]
            )
            for code_counts in code_count_batch
        ]
    )
    content = body.SerializeToString()
    return Mock(status_code=status_code, content=content, parsed=body)


def submit_response(request_ids: list[str], status_code: int = 200):
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
    backend.submit.side_effect = [submit_response(["1"])]

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
        submit_response([], status_code=503),
        submit_response(["a"], status_code=200),
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
        submit_response([], status_code=200),
    ]

    # when
    with pytest.raises(NeptuneSynchronizationStopped):
        sender_thread.work()

    # then should throw NeptuneInternalServerError
    errors_queue.put.assert_called_once()


def test_status_tracking_thread_processes_single_element():
    # given
    status_tracking_queue = Mock()
    errors_queue = Mock()
    last_ack_seq = SharedInt(initial_value=0)
    last_ack_timestamp = SharedFloat(initial_value=0)
    backend = Mock()
    status_tracking_thread = StatusTrackingThread(
        api_token="",
        mode="disabled",
        project="",
        errors_queue=errors_queue,
        status_tracking_queue=status_tracking_queue,
        last_ack_seq=last_ack_seq,
        last_ack_timestamp=last_ack_timestamp,
    )
    status_tracking_thread._backend = backend

    # and
    element = StatusTrackingElement(sequence_id=1, timestamp=time.process_time(), request_id="a")
    status_tracking_queue.peek.side_effect = [[element], queue.Empty]

    # and
    backend.check_batch.side_effect = [status_response(code_count_batch=[{"OK": 1}])]

    # when
    status_tracking_thread.work()

    # then
    assert backend.check_batch.call_count == 1
    assert last_ack_seq.value == 1
    assert last_ack_timestamp.value == element.timestamp


def test_status_tracking_thread_processes_element_on_single_retryable_error():
    # given
    status_tracking_queue = Mock()
    errors_queue = Mock()
    last_ack_seq = SharedInt(initial_value=0)
    last_ack_timestamp = SharedFloat(initial_value=0)
    backend = Mock()
    status_tracking_thread = StatusTrackingThread(
        api_token="",
        mode="disabled",
        project="",
        errors_queue=errors_queue,
        status_tracking_queue=status_tracking_queue,
        last_ack_seq=last_ack_seq,
        last_ack_timestamp=last_ack_timestamp,
    )
    status_tracking_thread._backend = backend

    # and
    element = StatusTrackingElement(sequence_id=1, timestamp=time.process_time(), request_id="a")
    status_tracking_queue.peek.side_effect = [[element], queue.Empty]

    # and
    backend.check_batch.side_effect = [
        status_response(code_count_batch=[], status_code=408),
        status_response(code_count_batch=[{"OK": 1}]),
    ]

    # when
    status_tracking_thread.work()

    # then
    assert backend.check_batch.call_count == 2
    assert last_ack_seq.value == 1
    assert last_ack_timestamp.value == element.timestamp


def test_status_tracking_thread_fails_on_regular_error():
    # given
    status_tracking_queue = Mock()
    errors_queue = Mock()
    last_ack_seq = SharedInt(initial_value=0)
    last_ack_timestamp = SharedFloat(initial_value=0)
    backend = Mock()
    status_tracking_thread = StatusTrackingThread(
        api_token="",
        mode="disabled",
        project="",
        errors_queue=errors_queue,
        status_tracking_queue=status_tracking_queue,
        last_ack_seq=last_ack_seq,
        last_ack_timestamp=last_ack_timestamp,
    )
    status_tracking_thread._backend = backend

    # and
    element = StatusTrackingElement(sequence_id=1, timestamp=time.process_time(), request_id="a")
    status_tracking_queue.peek.side_effect = [[element], queue.Empty]

    # and
    backend.check_batch.side_effect = [
        status_response(code_count_batch=[], status_code=403),
    ]

    # when
    with pytest.raises(NeptuneSynchronizationStopped):
        status_tracking_thread.work()

    # then
    errors_queue.put.assert_called_once()
    assert backend.check_batch.call_count == 1
    assert last_ack_seq.value == 0
    assert last_ack_timestamp.value == 0
