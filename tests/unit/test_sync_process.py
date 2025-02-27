import time
from unittest.mock import Mock

import neptune_api.proto.neptune_pb.ingest.v1.ingest_pb2 as ingest_pb2
import pytest
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
    UpdateRunSnapshot,
    Value,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.client_pb2 import SubmitResponse

from neptune_scale import NeptuneScaleWarning
from neptune_scale.exceptions import (
    NeptuneScaleError,
    NeptuneSynchronizationStopped,
    NeptuneUnexpectedError,
)
from neptune_scale.sync.operations_repository import (
    Metadata,
    Operation,
    OperationType,
    SequenceId,
)
from neptune_scale.sync.sync_process import (
    SenderThread,
    code_to_exception,
)
from neptune_scale.util.shared_var import SharedInt

metadata = Metadata(project="project", run_id="run_id", version="v1")


def response(request_ids: list[str], status_code: int = 200):
    body = SubmitResponse(request_ids=request_ids, request_id=request_ids[-1] if request_ids else None)
    content = body.SerializeToString()
    return Mock(status_code=status_code, content=content, parsed=body)


def single_operation(update: UpdateRunSnapshot, sequence_id):
    return Operation(
        sequence_id=SequenceId(sequence_id),
        timestamp=int(time.time() * 1000),
        operation_type=OperationType.UPDATE_SNAPSHOT,
        operation=update,
        operation_size_bytes=update.ByteSize(),
    )


@pytest.fixture
def operations_repository():
    repo = Mock()
    repo.get_metadata.side_effect = [metadata]
    return repo


def test_sender_thread_work_finishes_when_queue_empty(operations_repository):
    # given
    status_tracking_queue = Mock()
    errors_queue = Mock()
    last_queue_seq = SharedInt(initial_value=0)
    backend = Mock()
    sender_thread = SenderThread(
        api_token="",
        family="",
        operations_repository=operations_repository,
        status_tracking_queue=status_tracking_queue,
        errors_queue=errors_queue,
        last_queued_seq=last_queue_seq,
        mode="disabled",
    )
    sender_thread._backend = backend

    # and
    operations_repository.get_operations.side_effect = [[]]

    # when
    sender_thread.work()

    # then
    assert True


def test_sender_thread_processes_single_element(operations_repository):
    # given

    status_tracking_queue = Mock()
    errors_queue = Mock()
    last_queue_seq = SharedInt(initial_value=0)
    backend = Mock()
    sender_thread = SenderThread(
        api_token="",
        family="",
        operations_repository=operations_repository,
        status_tracking_queue=status_tracking_queue,
        errors_queue=errors_queue,
        last_queued_seq=last_queue_seq,
        mode="disabled",
    )
    sender_thread._backend = backend

    # and
    update = UpdateRunSnapshot(assign={"key": Value(string="a")})
    element = single_operation(update, sequence_id=2)
    operations_repository.get_operations.side_effect = [[element], []]

    # and
    backend.submit.side_effect = [response(["1"])]

    # when
    sender_thread.work()

    # then
    assert backend.submit.call_count == 1


def test_sender_thread_processes_element_on_single_retryable_error(operations_repository):
    # given
    status_tracking_queue = Mock()
    errors_queue = Mock()
    last_queue_seq = SharedInt(initial_value=0)
    backend = Mock()
    sender_thread = SenderThread(
        api_token="",
        family="",
        operations_repository=operations_repository,
        status_tracking_queue=status_tracking_queue,
        errors_queue=errors_queue,
        last_queued_seq=last_queue_seq,
        mode="disabled",
    )
    sender_thread._backend = backend

    # and
    update = UpdateRunSnapshot(assign={"key": Value(string="a")})
    element = single_operation(update, sequence_id=2)
    operations_repository.get_operations.side_effect = [[element], []]

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
    operations_repository = Mock()
    status_tracking_queue = Mock()
    errors_queue = Mock()
    last_queue_seq = SharedInt(initial_value=0)
    backend = Mock()
    sender_thread = SenderThread(
        api_token="",
        family="",
        operations_repository=operations_repository,
        status_tracking_queue=status_tracking_queue,
        errors_queue=errors_queue,
        last_queued_seq=last_queue_seq,
        mode="disabled",
    )
    sender_thread._backend = backend
    operations_repository.get_metadata.side_effect = [metadata]

    # and
    update = UpdateRunSnapshot(assign={"key": Value(string="a")})
    element = single_operation(update, sequence_id=2)
    operations_repository.get_operations.side_effect = [[element], []]

    # and
    backend.submit.side_effect = [
        response([], status_code=200),
    ]

    # when
    with pytest.raises(NeptuneSynchronizationStopped):
        sender_thread.work()

    # then should throw NeptuneInternalServerError
    errors_queue.put.assert_called_once()


def test_sender_thread_processes_element_on_429_and_408_http_statuses(operations_repository):
    # given
    status_tracking_queue = Mock()
    errors_queue = Mock()
    last_queue_seq = SharedInt(initial_value=0)
    backend = Mock()
    sender_thread = SenderThread(
        api_token="",
        family="",
        operations_repository=operations_repository,
        status_tracking_queue=status_tracking_queue,
        errors_queue=errors_queue,
        last_queued_seq=last_queue_seq,
        mode="disabled",
    )
    sender_thread._backend = backend

    # and
    update = UpdateRunSnapshot(assign={"key": Value(string="a")})
    element = single_operation(update, sequence_id=2)
    operations_repository.get_operations.side_effect = [[element], []]

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
