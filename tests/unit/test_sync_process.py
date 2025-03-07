import itertools
import os
import tempfile
import time
from pathlib import Path
from unittest.mock import Mock

import neptune_api.proto.neptune_pb.ingest.v1.ingest_pb2 as ingest_pb2
import pytest
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import Run as CreateRun
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
from neptune_scale.sync.errors_tracking import ErrorsQueue
from neptune_scale.sync.operations_repository import (
    Metadata,
    Operation,
    OperationsRepository,
    OperationType,
    SequenceId,
)
from neptune_scale.sync.parameters import MAX_REQUEST_SIZE_BYTES
from neptune_scale.sync.sync_process import (
    PeekableQueue,
    SenderThread,
    StatusTrackingElement,
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
def operations_repository_mock():
    repo = Mock()
    repo.get_metadata.side_effect = [metadata]
    return repo


@pytest.fixture
def operations_repo():
    with tempfile.TemporaryDirectory() as temp_dir:
        repo = OperationsRepository(db_path=Path(os.path.join(temp_dir, "test_operations.db")))
        repo.init_db()
        repo.save_metadata("project", "run_id")
        yield repo
        repo.close()


def test_sender_thread_work_finishes_when_queue_empty(operations_repository_mock):
    # given
    status_tracking_queue = Mock()
    errors_queue = Mock()
    last_queue_seq = SharedInt(initial_value=0)
    backend = Mock()
    sender_thread = SenderThread(
        api_token="",
        family="",
        operations_repository=operations_repository_mock,
        status_tracking_queue=status_tracking_queue,
        errors_queue=errors_queue,
        last_queued_seq=last_queue_seq,
        mode="disabled",
    )
    sender_thread._backend = backend

    # and
    operations_repository_mock.get_operations.side_effect = [[]]

    # when
    sender_thread.work()

    # then
    assert True


def test_sender_thread_processes_single_element(operations_repository_mock):
    # given

    status_tracking_queue = Mock()
    errors_queue = Mock()
    last_queue_seq = SharedInt(initial_value=0)
    backend = Mock()
    sender_thread = SenderThread(
        api_token="",
        family="",
        operations_repository=operations_repository_mock,
        status_tracking_queue=status_tracking_queue,
        errors_queue=errors_queue,
        last_queued_seq=last_queue_seq,
        mode="disabled",
    )
    sender_thread._backend = backend

    # and
    update = UpdateRunSnapshot(assign={"key": Value(string="a")})
    element = single_operation(update, sequence_id=2)
    operations_repository_mock.get_operations.side_effect = [[element], []]

    # and
    backend.submit.side_effect = [response(["1"])]

    # when
    sender_thread.work()

    # then
    assert backend.submit.call_count == 1


def test_sender_thread_processes_element_on_single_retryable_error(operations_repository_mock):
    # given
    status_tracking_queue = Mock()
    errors_queue = Mock()
    last_queue_seq = SharedInt(initial_value=0)
    backend = Mock()
    sender_thread = SenderThread(
        api_token="",
        family="",
        operations_repository=operations_repository_mock,
        status_tracking_queue=status_tracking_queue,
        errors_queue=errors_queue,
        last_queued_seq=last_queue_seq,
        mode="disabled",
    )
    sender_thread._backend = backend

    # and
    update = UpdateRunSnapshot(assign={"key": Value(string="a")})
    element = single_operation(update, sequence_id=2)
    operations_repository_mock.get_operations.side_effect = [[element], []]

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
    operations_repository_mock = Mock()
    status_tracking_queue = Mock()
    errors_queue = Mock()
    last_queue_seq = SharedInt(initial_value=0)
    backend = Mock()
    sender_thread = SenderThread(
        api_token="",
        family="",
        operations_repository=operations_repository_mock,
        status_tracking_queue=status_tracking_queue,
        errors_queue=errors_queue,
        last_queued_seq=last_queue_seq,
        mode="disabled",
    )
    sender_thread._backend = backend
    operations_repository_mock.get_metadata.side_effect = [metadata]

    # and
    update = UpdateRunSnapshot(assign={"key": Value(string="a")})
    element = single_operation(update, sequence_id=2)
    operations_repository_mock.get_operations.side_effect = [[element], []]

    # and
    backend.submit.side_effect = [
        response([], status_code=200),
    ]

    # when
    with pytest.raises(NeptuneSynchronizationStopped):
        sender_thread.work()

    # then should throw NeptuneInternalServerError
    errors_queue.put.assert_called_once()


def test_sender_thread_processes_element_on_429_and_408_http_statuses(operations_repository_mock):
    # given
    status_tracking_queue = Mock()
    errors_queue = Mock()
    last_queue_seq = SharedInt(initial_value=0)
    backend = Mock()
    sender_thread = SenderThread(
        api_token="",
        family="",
        operations_repository=operations_repository_mock,
        status_tracking_queue=status_tracking_queue,
        errors_queue=errors_queue,
        last_queued_seq=last_queue_seq,
        mode="disabled",
    )
    sender_thread._backend = backend

    # and
    update = UpdateRunSnapshot(assign={"key": Value(string="a")})
    element = single_operation(update, sequence_id=2)
    operations_repository_mock.get_operations.side_effect = [[element], []]

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


def test_sender_thread_processes_elements_with_multiple_operations_in_batch(operations_repo):
    status_tracking_queue = PeekableQueue()
    errors_queue = ErrorsQueue()
    last_queue_seq = SharedInt(initial_value=0)
    backend = Mock()
    sender_thread = SenderThread(
        api_token="a" * 10,
        family="test-family",
        operations_repository=operations_repo,
        status_tracking_queue=status_tracking_queue,
        errors_queue=errors_queue,
        last_queued_seq=last_queue_seq,
        mode="disabled",
    )
    sender_thread._backend = backend
    backend.submit.side_effect = itertools.repeat(response(["a"], status_code=200))

    # and
    updates = []
    for i in range(10):
        update = UpdateRunSnapshot(assign={"key": Value(string=f"a{i}")})
        updates.append(update)
    last_sequence_id = operations_repo.save_update_run_snapshots(updates)
    # when
    sender_thread.work()

    # then
    assert backend.submit.call_count == 1

    tracking: list[StatusTrackingElement] = status_tracking_queue.peek(10)  # type: ignore
    assert len(tracking) == 1
    assert tracking[0].sequence_id == last_sequence_id

    assert operations_repo.get_operations(MAX_REQUEST_SIZE_BYTES) == []
    assert last_queue_seq.value == last_sequence_id


def test_sender_thread_processes_elements_with_multiple_operations_in_batches(operations_repo):
    status_tracking_queue = PeekableQueue()
    errors_queue = ErrorsQueue()
    last_queue_seq = SharedInt(initial_value=0)
    backend = Mock()
    sender_thread = SenderThread(
        api_token="a" * 10,
        family="test-family",
        operations_repository=operations_repo,
        status_tracking_queue=status_tracking_queue,
        errors_queue=errors_queue,
        last_queued_seq=last_queue_seq,
        mode="disabled",
    )
    sender_thread._backend = backend
    backend.submit.side_effect = itertools.repeat(response(["a"], status_code=200))

    # and
    updates = [UpdateRunSnapshot(assign={"key": Value(string=f"a{i}")}) for i in range(10)]

    operations_repo.save_create_run(CreateRun(family="test-run-id", experiment_id="Test Run"))

    last_sequence_id = operations_repo.save_update_run_snapshots(updates)
    operations_repo.save_create_run(CreateRun(family="test-run-id", experiment_id="Test Run"))

    last_sequence_id = operations_repo.save_update_run_snapshots(updates)

    # when
    sender_thread.work()

    # then
    assert backend.submit.call_count == 4

    tracking: list[StatusTrackingElement] = status_tracking_queue.peek(10)
    assert len(tracking) == 4
    assert tracking[-1].sequence_id == last_sequence_id

    assert operations_repo.get_operations(MAX_REQUEST_SIZE_BYTES) == []
    assert last_queue_seq.value == last_sequence_id


def test_sender_thread_processes_big_operations_in_batches(operations_repo):
    status_tracking_queue = PeekableQueue()
    errors_queue = ErrorsQueue()
    last_queue_seq = SharedInt(initial_value=0)
    backend = Mock()
    sender_thread = SenderThread(
        api_token="a" * 10,
        family="test-family",
        operations_repository=operations_repo,
        status_tracking_queue=status_tracking_queue,
        errors_queue=errors_queue,
        last_queued_seq=last_queue_seq,
        mode="disabled",
    )
    sender_thread._backend = backend
    backend.submit.side_effect = itertools.repeat(response(["a"], status_code=200))

    # and
    operations_repo.save_create_run(CreateRun(family="test-run-id", experiment_id="Test Run"))

    updates = [UpdateRunSnapshot(assign={"key": Value(string="a" * 1024 * 1024)})] * 30  # 30MB
    last_sequence_id = operations_repo.save_update_run_snapshots(updates)

    # when
    sender_thread.work()

    # then
    assert backend.submit.call_count == 3

    tracking: list[StatusTrackingElement] = status_tracking_queue.peek(10)
    assert len(tracking) == 3
    assert tracking[-1].sequence_id == last_sequence_id

    assert operations_repo.get_operations(MAX_REQUEST_SIZE_BYTES) == []
    assert last_queue_seq.value == last_sequence_id


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
