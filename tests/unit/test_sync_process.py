import asyncio
import datetime
import itertools
import os
import pathlib
import tempfile
import time
from datetime import timedelta
from pathlib import Path
from unittest.mock import (
    ANY,
    AsyncMock,
    Mock,
    call,
    patch,
)

import neptune_api.proto.neptune_pb.ingest.v1.ingest_pb2 as ingest_pb2
import pytest
from azure.core.exceptions import (
    AzureError,
    ClientAuthenticationError,
    HttpResponseError,
)
from neptune_api.proto.google_rpc.code_pb2 import Code
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import Run as CreateRun
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
    UpdateRunSnapshot,
    Value,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.client_pb2 import (
    BulkRequestStatus,
    SubmitResponse,
)

from neptune_scale import NeptuneScaleWarning
from neptune_scale.exceptions import (
    NeptuneFileUploadError,
    NeptuneFileUploadTemporaryError,
    NeptuneScaleError,
    NeptuneSynchronizationStopped,
    NeptuneUnexpectedError,
)
from neptune_scale.sync.errors_tracking import ErrorsQueue
from neptune_scale.sync.operations_repository import (
    FileUploadRequest,
    Metadata,
    Operation,
    OperationsRepository,
    OperationType,
    SequenceId,
)
from neptune_scale.sync.parameters import MAX_REQUEST_SIZE_BYTES
from neptune_scale.sync.sync_process import (
    FileUploaderThread,
    PeekableQueue,
    SenderThread,
    StatusTrackingElement,
    StatusTrackingThread,
    code_to_exception,
)
from neptune_scale.util.shared_var import (
    SharedFloat,
    SharedInt,
)

metadata = Metadata(project="project", run_id="run_id")


def response(request_ids: list[str], status_code: int = 200):
    body = SubmitResponse(request_ids=request_ids, request_id=request_ids[-1] if request_ids else None)
    content = body.SerializeToString()
    return Mock(status_code=status_code, content=content, parsed=body)


def status_response(
    status_code: int = 200,
    pb_code: int = Code.OK,
    pb_detail: int = ingest_pb2.IngestCode.OK,
):
    body = BulkRequestStatus(statuses=[{"code_by_count": [{"code": pb_code, "count": 1, "detail": pb_detail}]}])
    content = body.SerializeToString()
    return Mock(status_code=status_code, content=content, parsed=body)


def status_response_batch(
    status_code: int = 200,
    statuses: list[tuple[int, int, int]] = [(Code.OK, ingest_pb2.IngestCode.OK, 1)],
):
    body = BulkRequestStatus(
        statuses=[
            {"code_by_count": [{"code": code, "count": count, "detail": detail}]} for code, detail, count in statuses
        ]
    )
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
        repo.close(cleanup_files=True)


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
    operations_repository_mock.get_metadata.side_effect = [metadata]
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
    )
    sender_thread._backend = backend

    # and
    update = UpdateRunSnapshot(assign={"key": Value(string="a")})
    element = single_operation(update, sequence_id=2)
    operations_repository_mock.get_operations.side_effect = [[element], []]

    # and
    backend.submit.side_effect = [
        response([], status_code=200),
    ]

    # when
    with pytest.raises(NeptuneSynchronizationStopped) as e:
        sender_thread.work()
    assert "Server response is empty" in str(e.value.__cause__)

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

    assert len(operations_repo.get_operations(MAX_REQUEST_SIZE_BYTES)) == 10
    assert last_queue_seq.value == last_sequence_id


def test_sender_thread_processes_elements_with_multiple_operations_split_by_type(operations_repo):
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

    assert operations_repo.get_sequence_id_range() == (1, last_sequence_id)
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

    assert operations_repo.get_sequence_id_range() == (1, last_sequence_id)
    assert last_queue_seq.value == last_sequence_id


def test_sender_thread_does_not_exceed_max_message_size_with_multiple_small_operations(operations_repo):
    """Verify if we calculate protobuf overhead properly for multiple small operations,
    so that the maximum message size is not exceeded."""
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
    )
    sender_thread._backend = backend

    def mock_submit(operation, family):
        assert len(operation.SerializeToString()) <= MAX_REQUEST_SIZE_BYTES
        return response(["a"], status_code=200)

    backend.submit.side_effect = mock_submit

    # and
    operations_repo.save_create_run(CreateRun(family="test-run-id", experiment_id="Test Run"))

    # Generate multiple small operations that don't fit in a single request
    small_op = UpdateRunSnapshot(assign={"small-operation": Value(int64=1)})
    single_op_size = len(small_op.SerializeToString())
    num_ops = 100 + MAX_REQUEST_SIZE_BYTES // single_op_size
    last_sequence_id = operations_repo.save_update_run_snapshots([small_op for _ in range(num_ops)])

    # when
    sender_thread.work()

    # then
    assert backend.submit.call_count > 1

    tracking: list[StatusTrackingElement] = status_tracking_queue.peek(10)
    assert len(tracking) == backend.submit.call_count
    assert tracking[-1].sequence_id == last_sequence_id

    assert operations_repo.get_sequence_id_range() == (1, last_sequence_id)
    assert last_queue_seq.value == last_sequence_id


def test_status_thread_processes_element():
    # given
    operations_repository = Mock()
    errors_queue = Mock()
    status_tracking_queue = Mock()
    last_ack_seq = SharedInt(initial_value=-1)
    last_ack_timestamp = SharedFloat(initial_value=-1)
    backend = Mock()
    status_thread = StatusTrackingThread(
        api_token="",
        project="",
        operations_repository=operations_repository,
        errors_queue=errors_queue,
        status_tracking_queue=status_tracking_queue,
        last_ack_seq=last_ack_seq,
        last_ack_timestamp=last_ack_timestamp,
    )
    status_thread._backend = backend

    # and
    status_element = StatusTrackingElement(
        sequence_id=SequenceId(0), timestamp=datetime.datetime.now(), request_id="id0"
    )
    status_tracking_queue.peek.side_effect = [[status_element], None]

    # and
    backend.check_batch.side_effect = [
        status_response(status_code=200),
    ]

    # when
    status_thread.work()

    # then
    operations_repository.delete_operations.assert_called_once_with(up_to_seq_id=SequenceId(0))
    errors_queue.put.assert_not_called()
    status_tracking_queue.commit.assert_called_once_with(1)
    assert last_ack_seq.value == status_element.sequence_id
    assert last_ack_timestamp.value == status_element.timestamp.timestamp()


@pytest.mark.parametrize(
    "detail",
    set(ingest_pb2.IngestCode.DESCRIPTOR.values_by_number.keys())
    - {
        ingest_pb2.IngestCode.PROJECT_INVALID_NAME,
        ingest_pb2.IngestCode.PROJECT_NOT_FOUND,
        ingest_pb2.IngestCode.RUN_NOT_FOUND,
        ingest_pb2.IngestCode.RUN_CONFLICTING,
        ingest_pb2.IngestCode.RUN_INVALID_CREATION_PARAMETERS,
    },
)
def test_status_thread_processes_element_with_standard_error_code(detail):
    # given
    operations_repository = Mock()
    errors_queue = Mock()
    status_tracking_queue = Mock()
    last_ack_seq = SharedInt(initial_value=-1)
    last_ack_timestamp = SharedFloat(initial_value=-1)
    backend = Mock()
    status_thread = StatusTrackingThread(
        api_token="",
        project="",
        operations_repository=operations_repository,
        errors_queue=errors_queue,
        status_tracking_queue=status_tracking_queue,
        last_ack_seq=last_ack_seq,
        last_ack_timestamp=last_ack_timestamp,
    )
    status_thread._backend = backend

    # and
    status_element = StatusTrackingElement(
        sequence_id=SequenceId(0), timestamp=datetime.datetime.now(), request_id="id0"
    )
    status_tracking_queue.peek.side_effect = [[status_element], None]

    # and
    backend.check_batch.side_effect = [
        status_response(status_code=200, pb_code=Code.ABORTED, pb_detail=detail),
    ]

    # when
    status_thread.work()

    # then
    errors_queue.put.assert_called_once()
    operations_repository.delete_operations.assert_called_once_with(up_to_seq_id=status_element.sequence_id)
    status_tracking_queue.commit.assert_called_once_with(1)
    assert last_ack_seq.value == status_element.sequence_id
    assert last_ack_timestamp.value == status_element.timestamp.timestamp()


@pytest.mark.parametrize(
    "detail",
    [
        ingest_pb2.IngestCode.PROJECT_INVALID_NAME,
        ingest_pb2.IngestCode.PROJECT_NOT_FOUND,
        ingest_pb2.IngestCode.RUN_NOT_FOUND,
        ingest_pb2.IngestCode.RUN_CONFLICTING,
        ingest_pb2.IngestCode.RUN_INVALID_CREATION_PARAMETERS,
    ],
)
def test_status_thread_processes_element_with_run_creation_error_code(detail):
    # given
    operations_repository = Mock()
    errors_queue = Mock()
    status_tracking_queue = Mock()
    last_ack_seq = SharedInt(initial_value=-1)
    last_ack_timestamp = SharedFloat(initial_value=-1)
    backend = Mock()
    status_thread = StatusTrackingThread(
        api_token="",
        project="",
        operations_repository=operations_repository,
        errors_queue=errors_queue,
        status_tracking_queue=status_tracking_queue,
        last_ack_seq=last_ack_seq,
        last_ack_timestamp=last_ack_timestamp,
    )
    status_thread._backend = backend

    # and
    status_element = StatusTrackingElement(
        sequence_id=SequenceId(0), timestamp=datetime.datetime.now(), request_id="id0"
    )
    status_tracking_queue.peek.side_effect = [[status_element], None]

    # and
    backend.check_batch.side_effect = [
        status_response(status_code=200, pb_code=Code.ABORTED, pb_detail=detail),
    ]

    # when
    with pytest.raises(NeptuneSynchronizationStopped):
        status_thread.work()

    # then
    errors_queue.put.assert_called_once()
    operations_repository.delete_operations.assert_not_called()
    status_tracking_queue.commit.assert_not_called()
    assert last_ack_seq.value == -1
    assert last_ack_timestamp.value == -1


def test_status_thread_processes_element_sequence():
    # given
    operations_repository = Mock()
    errors_queue = Mock()
    status_tracking_queue = Mock()
    last_ack_seq = SharedInt(initial_value=-1)
    last_ack_timestamp = SharedFloat(initial_value=-1)
    backend = Mock()
    status_thread = StatusTrackingThread(
        api_token="",
        project="",
        operations_repository=operations_repository,
        errors_queue=errors_queue,
        status_tracking_queue=status_tracking_queue,
        last_ack_seq=last_ack_seq,
        last_ack_timestamp=last_ack_timestamp,
    )
    status_thread._backend = backend

    # and
    timestamp = datetime.datetime.now()
    status_elements = [
        StatusTrackingElement(
            sequence_id=SequenceId(i), timestamp=timestamp + timedelta(seconds=i), request_id=f"id{i}"
        )
        for i in range(7)
    ]
    status_tracking_queue.peek.side_effect = [status_elements[:2], status_elements[2:4], status_elements[4:], None]

    # and
    backend.check_batch.side_effect = [
        status_response_batch(status_code=200, statuses=[(Code.OK, ingest_pb2.IngestCode.OK, 1)] * 2),
        status_response_batch(
            status_code=200,
            statuses=[
                (Code.ABORTED, ingest_pb2.IngestCode.SERIES_STEP_NON_INCREASING, 1),
                (Code.OK, ingest_pb2.IngestCode.OK, 1),
            ],
        ),
        status_response_batch(
            status_code=200,
            statuses=[
                (Code.OK, ingest_pb2.IngestCode.OK, 1),
                (Code.ABORTED, ingest_pb2.IngestCode.RUN_INVALID_CREATION_PARAMETERS, 1),
                (Code.OK, ingest_pb2.IngestCode.OK, 1),
            ],
        ),
        None,
    ]

    # when
    with pytest.raises(NeptuneSynchronizationStopped):
        status_thread.work()

    # then
    operations_repository.delete_operations.assert_has_calls(
        [
            call.method(up_to_seq_id=SequenceId(1)),
            call.method(up_to_seq_id=SequenceId(3)),
            call.method(up_to_seq_id=SequenceId(4)),
        ]
    )
    assert errors_queue.put.call_count == 2
    status_tracking_queue.commit.assert_has_calls(
        [
            call.method(2),
            call.method(2),
            call.method(1),
        ]
    )
    assert last_ack_seq.value == status_elements[-3].sequence_id
    assert last_ack_timestamp.value == status_elements[-3].timestamp.timestamp()


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


@pytest.fixture
def mock_backend_factory():
    with patch("neptune_scale.sync.sync_process.backend_factory") as mock:
        yield mock


@pytest.fixture
def mock_fetch_file_storage_urls():
    with patch("neptune_scale.sync.sync_process.fetch_file_storage_urls") as mock:
        mock.return_value = {"target/text.txt": "text-url", "target/image.jpg": "image-url"}
        yield mock


@pytest.fixture
def mock_upload_file():
    with patch("neptune_scale.sync.sync_process.upload_file") as mock:
        yield mock


@pytest.fixture
def mock_operations_repository():
    return Mock(spec=OperationsRepository)


@pytest.fixture
def mock_errors_queue():
    return Mock(spec=ErrorsQueue)


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield pathlib.Path(temp_dir)


@pytest.fixture
def buffer_upload_request(temp_dir):
    filename = str(temp_dir / "text.txt")
    data = "test"
    with open(filename, "w") as f:
        f.write(data)

    request = FileUploadRequest(filename, "target/text.txt", "text/plain", len(data), True, SequenceId(1))
    return request


@pytest.fixture
def disk_upload_request(temp_dir):
    filename = str(temp_dir / "image.jpg")
    data = "test"
    with open(filename, "w") as f:
        f.write(data)

    request = FileUploadRequest(filename, "target/image.jpg", "image/jpeg", len(data), False, SequenceId(2))
    return request


@pytest.fixture
def uploader_thread(
    api_token,
    mock_backend_factory,
    mock_operations_repository,
    mock_errors_queue,
):
    thread = FileUploaderThread(
        project="workspace/project",
        api_token=api_token,
        operations_repository=mock_operations_repository,
        errors_queue=mock_errors_queue,
    )

    yield thread

    if thread.is_alive():
        thread.interrupt()
        thread.join()


def test_file_uploader_thread_successful_upload_flow(
    temp_dir,
    uploader_thread,
    mock_fetch_file_storage_urls,
    mock_upload_file,
    mock_operations_repository,
    mock_errors_queue,
    buffer_upload_request,
    disk_upload_request,
):
    """Test the happy path flow of the file uploader thread."""

    mock_operations_repository.get_file_upload_requests.side_effect = [
        [buffer_upload_request, disk_upload_request],
        [],
        [],
    ]

    uploader_thread.start()
    uploader_thread.interrupt(remaining_iterations=1)
    uploader_thread.join()

    assert mock_operations_repository.get_file_upload_requests.called

    # Storage URLs should be fetched for all files
    mock_fetch_file_storage_urls.assert_called_once_with(
        uploader_thread._api_client, "workspace/project", ["target/text.txt", "target/image.jpg"]
    )

    expected_calls = [
        call(
            buffer_upload_request.source_path,
            buffer_upload_request.mime_type,
            "text-url",
            ANY,
        ),
        call(
            disk_upload_request.source_path,
            disk_upload_request.mime_type,
            "image-url",
            ANY,
        ),
    ]
    # All files should be uploaded
    mock_upload_file.assert_has_calls(expected_calls)

    # Completed files should be deleted from repository
    mock_operations_repository.delete_file_upload_requests.assert_has_calls(
        [call([buffer_upload_request.sequence_id]), call([disk_upload_request.sequence_id])]
    )

    # Temporary files should be deleted, user files should be not
    assert not pathlib.Path(buffer_upload_request.source_path).exists()
    assert pathlib.Path(disk_upload_request.source_path).exists()

    # No errors should be emitted
    mock_errors_queue.put.assert_not_called()


def test_file_uploader_uploads_concurrently(
    api_token,
    mock_backend_factory,
    mock_operations_repository,
    mock_errors_queue,
    mock_upload_file,
    mock_fetch_file_storage_urls,
):
    """Verify that FileUploaderThread uploads concurrently, and respects the max_concurrent_uploads limit."""

    thread = FileUploaderThread(
        project="workspace/project",
        api_token=api_token,
        operations_repository=mock_operations_repository,
        errors_queue=mock_errors_queue,
        max_concurrent_uploads=3,
    )

    lock = asyncio.Lock()
    concurrent_uploads = 0
    peak_uploads = 0

    async def _upload_file(local_path, mime_type, storage_url, transport):
        """Track the number of concurrent uploads and the peak number of concurrent uploads."""
        nonlocal peak_uploads, concurrent_uploads

        async with lock:
            concurrent_uploads += 1
            peak_uploads = max(peak_uploads, concurrent_uploads)

        await asyncio.sleep(0.5)

        async with lock:
            concurrent_uploads -= 1

    mock_upload_file.side_effect = _upload_file

    mock_operations_repository.get_file_upload_requests.side_effect = [
        [FileUploadRequest("no-such-file", "target/text.txt", "text/plain", 123, False, SequenceId(1))],
        [
            # This is the peak batch
            FileUploadRequest("no-such-file", "target/text.txt", "text/plain", 123, False, SequenceId(2)),
            FileUploadRequest("no-such-file", "target/text.txt", "text/plain", 123, False, SequenceId(3)),
            FileUploadRequest("no-such-file", "target/text.txt", "text/plain", 123, False, SequenceId(4)),
        ],
        [FileUploadRequest("no-such-file", "target/text.txt", "text/plain", 123, False, SequenceId(5))],
        [
            FileUploadRequest("no-such-file", "target/text.txt", "text/plain", 123, False, SequenceId(6)),
            FileUploadRequest("no-such-file", "target/text.txt", "text/plain", 123, False, SequenceId(7)),
        ],
        [],
    ]

    thread.start()
    thread.interrupt(remaining_iterations=4)
    thread.join()

    # Assert that we only pull the max concurrent number of uploads from the repository
    mock_operations_repository.get_file_upload_requests.assert_has_calls([call(3), call(3), call(3), call(3)])
    assert peak_uploads == 3, f"Peak concurrent uploads should be 3, but was {peak_uploads}"
    assert mock_upload_file.call_count == 7


def test_file_uploader_thread_terminal_error(
    temp_dir,
    uploader_thread,
    mock_fetch_file_storage_urls,
    mock_upload_file,
    mock_operations_repository,
    mock_errors_queue,
    disk_upload_request,
):
    """Uploader thread should not terminate on an upload error that is not an Azure error. This should
    be treated as a terminal error for the given upload, but further uploads should be processed."""

    mock_operations_repository.get_file_upload_requests.side_effect = [
        [
            FileUploadRequest("no-such-file", "target/text.txt", "text/plain", 123, False, SequenceId(11)),
            disk_upload_request,
        ],
        [],
        [],
    ]

    mock_upload_file.side_effect = [FileNotFoundError, None]

    uploader_thread.start()
    uploader_thread.interrupt(remaining_iterations=1)
    uploader_thread.join()

    mock_fetch_file_storage_urls.assert_called_once_with(
        uploader_thread._api_client, "workspace/project", ["target/text.txt", "target/image.jpg"]
    )

    # An upload attempt should be made for both files
    expected_calls = [
        call("no-such-file", "text/plain", "text-url", ANY),
        call(disk_upload_request.source_path, disk_upload_request.mime_type, "image-url", ANY),
    ]
    mock_upload_file.assert_has_calls(expected_calls)

    # Both files should be deleted from the repository
    mock_operations_repository.delete_file_upload_requests.assert_has_calls(
        [call([11]), call([disk_upload_request.sequence_id])]
    )

    assert pathlib.Path(disk_upload_request.source_path).exists()

    assert mock_errors_queue.put.call_count == 1
    assert isinstance(mock_errors_queue.put.call_args[0][0], NeptuneFileUploadError)


@patch("neptune_scale.sync.sync_process.BlobClient")
@pytest.mark.parametrize("upload_error", [AzureError(""), HttpResponseError, ClientAuthenticationError])
def test_file_uploader_thread_non_terminal_error(
    mock_blob_client_cls,
    upload_error,
    temp_dir,
    uploader_thread,
    mock_fetch_file_storage_urls,
    mock_operations_repository,
    mock_errors_queue,
    disk_upload_request,
):
    """Uploader thread should retry uploads on any Azure errors."""

    mock_blob_client = AsyncMock()
    mock_blob_client.upload_blob.side_effect = upload_error
    mock_blob_client_cls.from_blob_url.return_value = mock_blob_client

    # We will fail 2 times before succeeding
    mock_blob_client.from_blob_url.return_value = mock_blob_client
    mock_blob_client.upload_blob.side_effect = [upload_error, upload_error, None]

    mock_operations_repository.get_file_upload_requests.side_effect = [
        [disk_upload_request],
        [disk_upload_request],
        [disk_upload_request],
        [],
        [],
    ]

    uploader_thread.start()
    uploader_thread.interrupt(remaining_iterations=1)
    uploader_thread.join()

    # Signed urls should be requested for each attempt
    mock_fetch_file_storage_urls.assert_has_calls(
        [call(uploader_thread._api_client, "workspace/project", [disk_upload_request.destination])] * 3
    )

    # An upload attempt should be made for 2 failures and the final success
    assert mock_blob_client.upload_blob.call_count == 3

    # The file request should be deleted from the repository only once
    mock_operations_repository.delete_file_upload_requests.assert_called_once_with([disk_upload_request.sequence_id])

    # Two NeptuneFileUploadErrors should be reported
    assert mock_errors_queue.put.call_count == 2
    assert all(
        isinstance(call_args.args[0], NeptuneFileUploadTemporaryError)
        for call_args in mock_errors_queue.put.call_args_list
    )
