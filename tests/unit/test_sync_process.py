import asyncio
import dataclasses
import datetime
import itertools
import multiprocessing
import pathlib
import tempfile
import time
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
    NeptuneRetryableError,
    NeptuneScaleError,
    NeptuneSynchronizationStopped,
    NeptuneUnexpectedError,
)
from neptune_scale.sync.operations_repository import (
    FileUploadRequest,
    Metadata,
    Operation,
    OperationsRepository,
    OperationSubmission,
    OperationType,
    RequestId,
    SequenceId,
)
from neptune_scale.sync.parameters import MAX_REQUEST_SIZE_BYTES
from neptune_scale.sync.sync_process import (
    FileUploaderThread,
    SenderThread,
    StatusTrackingThread,
    code_to_exception,
    upload_to_azure,
)

metadata = Metadata(project="project", run_id="run_id")
mp_context = multiprocessing.get_context("spawn")


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
    repo.get_operation_submission_sequence_id_range.return_value = None
    return repo


def test_sender_thread_work_finishes_when_queue_empty(operations_repository_mock):
    # given
    backend = Mock()
    sender_thread = SenderThread(
        api_token="",
        family="",
        operations_repository=operations_repository_mock,
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
    backend = Mock()
    sender_thread = SenderThread(
        api_token="",
        family="",
        operations_repository=operations_repository_mock,
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
    backend = Mock()
    sender_thread = SenderThread(
        api_token="",
        family="",
        operations_repository=operations_repository_mock,
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
    operations_repository_mock.get_operation_submission_sequence_id_range.return_value = None
    backend = Mock()
    sender_thread = SenderThread(
        api_token="",
        family="",
        operations_repository=operations_repository_mock,
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
    operations_repository_mock.save_errors.assert_called_once()


def test_sender_thread_processes_element_on_429_and_408_http_statuses(operations_repository_mock):
    # given
    backend = Mock()
    sender_thread = SenderThread(
        api_token="",
        family="",
        operations_repository=operations_repository_mock,
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
    operations_repo.save_metadata("project", "run_id")
    backend = Mock()
    sender_thread = SenderThread(
        api_token="a" * 10,
        family="test-family",
        operations_repository=operations_repo,
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

    tracking: list[OperationSubmission] = operations_repo.get_operation_submissions(10)
    assert len(tracking) == 1
    assert tracking[0].sequence_id == last_sequence_id

    assert len(operations_repo.get_operations(MAX_REQUEST_SIZE_BYTES)) == 10


def test_sender_thread_processes_elements_with_nonempty_submissions(operations_repo):
    # given
    updates = []
    for i in range(10):
        update = UpdateRunSnapshot(assign={"key": Value(string=f"a{i}")})
        updates.append(update)
    last_sequence_id = operations_repo.save_update_run_snapshots(updates)
    submissions = [
        OperationSubmission(
            sequence_id=SequenceId(last_sequence_id),
            timestamp=int(datetime.datetime.now().timestamp() / 1000),
            request_id=RequestId("id10"),
        )
    ]
    operations_repo.save_operation_submissions(submissions)

    # and
    operations_repo.save_metadata("project", "run_id")
    backend = Mock()
    sender_thread = SenderThread(
        api_token="a" * 10,
        family="test-family",
        operations_repository=operations_repo,
    )
    sender_thread._backend = backend
    backend.submit.side_effect = itertools.repeat(response(["a"], status_code=200))

    # when
    sender_thread.work()

    # then
    assert backend.submit.call_count == 0

    tracking: list[OperationSubmission] = operations_repo.get_operation_submissions(10)
    assert len(tracking) == 1
    assert tracking[0].sequence_id == last_sequence_id


def test_sender_thread_processes_elements_with_nonempty_submissions_partial(operations_repo):
    # given
    updates = []
    for i in range(10):
        update = UpdateRunSnapshot(assign={"key": Value(string=f"a{i}")})
        updates.append(update)
    last_sequence_id = operations_repo.save_update_run_snapshots(updates)
    submissions = [
        OperationSubmission(
            sequence_id=SequenceId(5),
            timestamp=int(datetime.datetime.now().timestamp() / 1000),
            request_id=RequestId("id5"),
        )
    ]
    operations_repo.save_operation_submissions(submissions)

    # and
    operations_repo.save_metadata("project", "run_id")
    backend = Mock()
    sender_thread = SenderThread(
        api_token="a" * 10,
        family="test-family",
        operations_repository=operations_repo,
    )
    sender_thread._backend = backend
    backend.submit.side_effect = itertools.repeat(response(["a"], status_code=200))

    # when
    sender_thread.work()

    # then
    assert backend.submit.call_count == 1

    tracking: list[OperationSubmission] = operations_repo.get_operation_submissions(10)
    assert len(tracking) == 2
    assert [item.sequence_id for item in tracking] == [SequenceId(5), SequenceId(last_sequence_id)]


def test_sender_thread_processes_elements_with_multiple_operations_split_by_type(operations_repo):
    operations_repo.save_metadata("project", "run_id")
    backend = Mock()
    sender_thread = SenderThread(
        api_token="a" * 10,
        family="test-family",
        operations_repository=operations_repo,
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

    tracking: list[OperationSubmission] = operations_repo.get_operation_submissions(10)
    assert len(tracking) == 4
    assert tracking[-1].sequence_id == last_sequence_id

    assert operations_repo.get_operations_sequence_id_range() == (1, last_sequence_id)


def test_sender_thread_processes_big_operations_in_batches(operations_repo):
    operations_repo.save_metadata("project", "run_id")
    backend = Mock()
    sender_thread = SenderThread(
        api_token="a" * 10,
        family="test-family",
        operations_repository=operations_repo,
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

    tracking: list[OperationSubmission] = operations_repo.get_operation_submissions(10)
    assert len(tracking) == 3
    assert tracking[-1].sequence_id == last_sequence_id

    assert operations_repo.get_operations_sequence_id_range() == (1, last_sequence_id)


def test_sender_thread_does_not_exceed_max_message_size_with_multiple_small_operations(operations_repo):
    """Verify if we calculate protobuf overhead properly for multiple small operations,
    so that the maximum message size is not exceeded."""
    operations_repo.save_metadata("project", "run_id")
    backend = Mock()
    sender_thread = SenderThread(
        api_token="a" * 10,
        family="test-family",
        operations_repository=operations_repo,
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

    tracking: list[OperationSubmission] = operations_repo.get_operation_submissions(10)
    assert len(tracking) == backend.submit.call_count
    assert tracking[-1].sequence_id == last_sequence_id

    assert operations_repo.get_operations_sequence_id_range() == (1, last_sequence_id)


def test_status_thread_processes_element():
    # given
    operations_repository = Mock()
    backend = Mock()
    status_thread = StatusTrackingThread(
        api_token="",
        project="",
        operations_repository=operations_repository,
    )
    status_thread._backend = backend

    # and
    status_element = OperationSubmission(
        sequence_id=SequenceId(0),
        timestamp=int(datetime.datetime.now().timestamp() / 1000),
        request_id=RequestId("id0"),
    )
    operations_repository.get_operation_submissions.side_effect = [[status_element], None]

    # and
    backend.check_batch.side_effect = [
        status_response(status_code=200),
    ]

    # when
    status_thread.work()

    # then
    operations_repository.delete_operations.assert_called_once_with(up_to_seq_id=SequenceId(0))
    operations_repository.save_errors.assert_not_called()
    operations_repository.delete_operation_submissions.assert_called_once_with(up_to_seq_id=SequenceId(0))


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
    backend = Mock()
    status_thread = StatusTrackingThread(
        api_token="",
        project="",
        operations_repository=operations_repository,
    )
    status_thread._backend = backend

    # and
    status_element = OperationSubmission(
        sequence_id=SequenceId(0),
        timestamp=int(datetime.datetime.now().timestamp() / 1000),
        request_id=RequestId("id0"),
    )
    operations_repository.get_operation_submissions.side_effect = [[status_element], None]

    # and
    backend.check_batch.side_effect = [
        status_response(status_code=200, pb_code=Code.ABORTED, pb_detail=detail),
    ]

    # when
    status_thread.work()

    # then
    operations_repository.save_errors.assert_called_once()
    operations_repository.delete_operations.assert_called_once_with(up_to_seq_id=status_element.sequence_id)
    operations_repository.delete_operation_submissions.assert_called_once_with(up_to_seq_id=status_element.sequence_id)


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
    backend = Mock()
    status_thread = StatusTrackingThread(
        api_token="",
        project="",
        operations_repository=operations_repository,
    )
    status_thread._backend = backend

    # and
    status_element = OperationSubmission(
        sequence_id=SequenceId(0),
        timestamp=int(datetime.datetime.now().timestamp() / 1000),
        request_id=RequestId("id0"),
    )
    operations_repository.get_operation_submissions.side_effect = [[status_element], None]

    # and
    backend.check_batch.side_effect = [
        status_response(status_code=200, pb_code=Code.ABORTED, pb_detail=detail),
    ]

    # when
    with pytest.raises(NeptuneSynchronizationStopped):
        status_thread.work()

    # then
    operations_repository.save_errors.assert_called_once()
    operations_repository.delete_operations.assert_not_called()
    operations_repository.delete_operation_submissions.assert_not_called()


def test_status_thread_processes_element_sequence():
    # given
    operations_repository = Mock()
    backend = Mock()
    status_thread = StatusTrackingThread(
        api_token="",
        project="",
        operations_repository=operations_repository,
    )
    status_thread._backend = backend

    # and
    timestamp = datetime.datetime.now()
    status_elements = [
        OperationSubmission(
            sequence_id=SequenceId(i),
            timestamp=int(timestamp.timestamp() / 1000),
            request_id=RequestId(f"id{i}"),
        )
        for i in range(7)
    ]
    operations_repository.get_operation_submissions.side_effect = [
        status_elements[:2],
        status_elements[2:4],
        status_elements[4:],
        None,
    ]

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
    assert operations_repository.save_errors.call_count == 2
    operations_repository.delete_operation_submissions.assert_has_calls(
        [
            call.method(up_to_seq_id=SequenceId(1)),
            call.method(up_to_seq_id=SequenceId(3)),
            call.method(up_to_seq_id=SequenceId(4)),
        ]
    )


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
def mock_api_client():
    with patch("neptune_scale.sync.sync_process.ApiClient") as mock:
        yield mock


@pytest.fixture(params=["azure", "gcp"])
def provider(request):
    yield request.param


@pytest.fixture
def mock_fetch_file_storage_urls(provider):
    with patch("neptune_scale.sync.sync_process.fetch_file_storage_urls") as mock:
        mock.return_value = {"target/text.txt": (provider, "text-url"), "target/image.jpg": (provider, "image-url")}
        yield mock


@pytest.fixture
def mock_upload_func(provider):
    with (
        patch("neptune_scale.sync.sync_process.upload_to_azure") as mock_azure,
        patch("neptune_scale.sync.sync_process.upload_to_gcp") as mock_gcp,
    ):
        if provider == "azure":
            yield mock_azure
            mock_gcp.assert_not_called()
        else:
            yield mock_gcp
            mock_azure.assert_not_called()


@pytest.fixture
def mock_operations_repository():
    return Mock(spec=OperationsRepository)


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
    mock_api_client,
    mock_operations_repository,
):
    thread = FileUploaderThread(
        project="workspace/project",
        api_token=api_token,
        operations_repository=mock_operations_repository,
    )

    yield thread

    if thread.is_alive():
        thread.interrupt()
        thread.join()


def test_file_uploader_thread_successful_upload_flow(
    temp_dir,
    uploader_thread,
    mock_fetch_file_storage_urls,
    mock_upload_func,
    mock_operations_repository,
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
        call(buffer_upload_request.source_path, buffer_upload_request.mime_type, "text-url", chunk_size=ANY),
        call(disk_upload_request.source_path, disk_upload_request.mime_type, "image-url", chunk_size=ANY),
    ]
    # All files should be uploaded
    mock_upload_func.assert_has_calls(expected_calls)

    # Completed files should be deleted from repository
    mock_operations_repository.delete_file_upload_requests.assert_has_calls(
        [call([buffer_upload_request.sequence_id]), call([disk_upload_request.sequence_id])]
    )

    # Temporary files should be deleted, user files should be not
    assert not pathlib.Path(buffer_upload_request.source_path).exists()
    assert pathlib.Path(disk_upload_request.source_path).exists()

    # No errors should be emitted
    mock_operations_repository.save_errors.assert_not_called()


def test_file_uploader_uploads_concurrently(
    api_token,
    mock_api_client,
    mock_operations_repository,
    mock_upload_func,
    mock_fetch_file_storage_urls,
):
    """Verify that FileUploaderThread uploads concurrently, and respects the max_concurrent_uploads limit."""

    thread = FileUploaderThread(
        project="workspace/project",
        api_token=api_token,
        operations_repository=mock_operations_repository,
        max_concurrent_uploads=3,
    )

    lock = asyncio.Lock()
    concurrent_uploads = 0
    peak_uploads = 0

    async def _upload_file(local_path, mime_type, storage_url, chunk_size):
        """Track the number of concurrent uploads and the peak number of concurrent uploads."""
        nonlocal peak_uploads, concurrent_uploads

        async with lock:
            concurrent_uploads += 1
            peak_uploads = max(peak_uploads, concurrent_uploads)

        await asyncio.sleep(0.5)

        async with lock:
            concurrent_uploads -= 1

    mock_upload_func.side_effect = _upload_file

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
        [],
    ]

    thread.start()
    thread.interrupt(remaining_iterations=1)
    thread.join()

    # Assert that we only pull the max concurrent number of uploads from the repository
    mock_operations_repository.get_file_upload_requests.assert_has_calls([call(3), call(3), call(3), call(3)])
    assert peak_uploads == 3, f"Peak concurrent uploads should be 3, but was {peak_uploads}"
    assert mock_upload_func.call_count == 7


def test_file_uploader_thread_terminal_error(
    temp_dir,
    uploader_thread,
    mock_fetch_file_storage_urls,
    mock_upload_func,
    mock_operations_repository,
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

    mock_upload_func.side_effect = [FileNotFoundError, None]

    uploader_thread.start()
    uploader_thread.interrupt(remaining_iterations=1)
    uploader_thread.join()

    mock_fetch_file_storage_urls.assert_called_once_with(
        uploader_thread._api_client, "workspace/project", ["target/text.txt", "target/image.jpg"]
    )

    # An upload attempt should be made for both files
    expected_calls = [
        call("no-such-file", "text/plain", "text-url", chunk_size=ANY),
        call(disk_upload_request.source_path, disk_upload_request.mime_type, "image-url", chunk_size=ANY),
    ]
    mock_upload_func.assert_has_calls(expected_calls)

    # Both files should be deleted from the repository
    mock_operations_repository.delete_file_upload_requests.assert_has_calls(
        [call([11]), call([disk_upload_request.sequence_id])]
    )

    assert pathlib.Path(disk_upload_request.source_path).exists()

    assert mock_operations_repository.save_errors.call_count == 1
    assert isinstance(mock_operations_repository.save_errors.call_args[0][0][0], NeptuneFileUploadError)


@patch("neptune_scale.sync.sync_process.BlobClient")
@pytest.mark.parametrize(
    "upload_error, is_temporary",
    [
        (AzureError(""), True),
        (HttpResponseError, True),
        (ClientAuthenticationError, True),
        (ValueError, False),
        (FileNotFoundError, False),
        (PermissionError, False),
    ],
)
def test_upload_to_azure_errors(mock_blob_client_cls, upload_error, is_temporary, disk_upload_request):
    """Azure upload errors should raise NeptuneFileUploadTemporaryError for temporary errors,
    and re-raise other errors."""

    mock_blob_client = AsyncMock()
    mock_blob_client.upload_blob.side_effect = upload_error
    mock_blob_client_cls.from_blob_url.return_value = mock_blob_client

    mock_blob_client.from_blob_url.return_value = mock_blob_client
    mock_blob_client.upload_blob.side_effect = [upload_error, upload_error, None]

    with pytest.raises(Exception) as exc:
        asyncio.run(upload_to_azure(disk_upload_request.source_path, disk_upload_request.mime_type, "image-url"))

    if is_temporary:
        assert isinstance(exc.value, NeptuneFileUploadTemporaryError)
    else:
        assert not isinstance(exc.value, type(upload_error))


@pytest.mark.parametrize("upload_error", [NeptuneFileUploadTemporaryError(), NeptuneRetryableError()])
def test_file_uploader_thread_non_terminal_error(
    upload_error,
    temp_dir,
    uploader_thread,
    mock_fetch_file_storage_urls,
    mock_operations_repository,
    mock_upload_func,
    disk_upload_request,
):
    """Uploader thread should retry uploads on any non-terminal errors."""

    # We will fail 2 times before succeeding
    mock_upload_func.side_effect = [upload_error, upload_error, None]

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
    assert mock_upload_func.call_count == 3

    # The file request should be deleted from the repository only once
    mock_operations_repository.delete_file_upload_requests.assert_called_once_with([disk_upload_request.sequence_id])

    # Two errors of the type that was raised during upload should be reported
    assert mock_operations_repository.save_errors.call_count == 2
    assert all(
        isinstance(call_args.args[0][0], type(upload_error))
        for call_args in mock_operations_repository.save_errors.call_args_list
    )


@patch("neptune_scale.sync.sync_process.upload_to_gcp")
@patch("neptune_scale.sync.sync_process.upload_to_azure")
def test_file_uploader_thread_uploads_to_correct_provider(
    mock_upload_to_azure, mock_upload_to_gcp, uploader_thread, mock_operations_repository, disk_upload_request
):
    """If Neptune storage API returns multiple providers in a single response, FileUploaderThread should
    still call the correct upload function for each provider."""

    with patch("neptune_scale.sync.sync_process.fetch_file_storage_urls") as mock_fetch_urls:
        mock_fetch_urls.return_value = {
            "azure-1.jpg": ("azure", "azure-url-1"),
            "gcp-1.jpg": ("gcp", "gcp-url-1"),
            "azure-2.jpg": ("azure", "azure-url-2"),
            "gcp-2.jpg": ("gcp", "gcp-url-2"),
        }

        # The disk_upload_request is not a temporary file, so use it to create upload requests for
        # attributes defined above
        file_upload_requests = []
        for seq, path in enumerate(mock_fetch_urls.return_value.keys(), start=10):
            file_upload_requests.append(
                dataclasses.replace(disk_upload_request, destination=path, sequence_id=SequenceId(seq))
            )

        mock_operations_repository.get_file_upload_requests.side_effect = [
            file_upload_requests,
            [],
        ]

        uploader_thread.start()
        uploader_thread.interrupt(remaining_iterations=1)
        uploader_thread.join()

        local_path = disk_upload_request.source_path
        mime_type = disk_upload_request.mime_type

        assert mock_upload_to_azure.call_count == 2
        mock_upload_to_azure.assert_has_calls(
            [
                call(local_path, mime_type, "azure-url-1", chunk_size=ANY),
                call(local_path, mime_type, "azure-url-2", chunk_size=ANY),
            ]
        )

        assert mock_upload_to_gcp.call_count == 2
        mock_upload_to_gcp.assert_has_calls(
            [
                call(local_path, mime_type, "gcp-url-1", chunk_size=ANY),
                call(local_path, mime_type, "gcp-url-2", chunk_size=ANY),
            ]
        )
