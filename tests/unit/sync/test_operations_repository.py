import contextlib
import dataclasses
import os
import sqlite3
import tempfile
from pathlib import Path
from unittest import mock

import pytest
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import Run as CreateRun
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
    UpdateRunSnapshot,
    Value,
)

from neptune_scale.exceptions import (
    NeptuneLocalStorageInUnsupportedVersion,
    NeptuneUnableToLogData,
)
from neptune_scale.sync.operations_repository import (
    FileUploadRequest,
    Metadata,
    OperationsRepository,
    OperationType,
    SequenceId,
)
from neptune_scale.sync.parameters import MAX_SINGLE_OPERATION_SIZE_BYTES
from neptune_scale.util import envs


@pytest.fixture
def temp_db_path():
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, "test_operations.db")
        yield db_path


@pytest.fixture
def operations_repo(temp_db_path):
    repo = OperationsRepository(db_path=Path(temp_db_path))
    repo.init_db()
    yield repo
    repo.close(cleanup_files=True)


def test_raise_on_relative_path():
    with pytest.raises(RuntimeError, match="db_path must be .* absolute"):
        OperationsRepository(db_path=Path("relative/path"))


def test_init_creates_tables(temp_db_path):
    # When
    repo = OperationsRepository(db_path=Path(temp_db_path))
    repo.init_db()
    # Then
    conn = sqlite3.connect(temp_db_path)
    cursor = conn.cursor()

    # Check run_operations table
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='run_operations'")
    assert cursor.fetchone() is not None

    # Check metadata table
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='metadata'")
    assert cursor.fetchone() is not None

    # Cleanup
    conn.close()
    repo.close(cleanup_files=True)


def test_init_fails_with_neptune_exception(temp_db_path):
    # Given
    with open(temp_db_path, "wb") as f:
        f.write(b"corrupted data")

    # Then
    repo = OperationsRepository(db_path=Path(temp_db_path))

    with pytest.raises(NeptuneUnableToLogData) as exc:
        repo.init_db()
    assert "file is not a database" in str(exc.value.__cause__)

    # Cleanup
    os.remove(temp_db_path)


def test_save_update_run_snapshots(operations_repo, temp_db_path):
    # Given
    snapshots = []
    for i in range(3):
        snapshot = UpdateRunSnapshot(assign={f"key_{i}": Value(string=f"value_{i}")})
        snapshots.append(snapshot)

    # When
    operations_repo.save_update_run_snapshots(snapshots)

    # Then
    count = get_operation_count(temp_db_path)
    assert count == 3

    # Verify operation type
    conn = sqlite3.connect(operations_repo._db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT operation_type FROM run_operations")
    operation_types = cursor.fetchall()
    conn.close()

    assert all(op_type[0] == OperationType.UPDATE_SNAPSHOT for op_type in operation_types)


def test_save_update_run_snapshots_empty_list(operations_repo, temp_db_path):
    # Given
    snapshots = []

    # When
    sequence_id = operations_repo.save_update_run_snapshots(snapshots)

    # Then
    count = get_operation_count(temp_db_path)
    assert sequence_id == -1
    assert count == 0


def test_save_create_run(operations_repo, temp_db_path):
    # Given
    run = CreateRun(family="test-run-id", experiment_id="Test Run")

    # When
    operations_repo.save_create_run(run)

    # Then
    count = get_operation_count(temp_db_path)
    assert count == 1

    operation = operations_repo.get_operations(up_to_bytes=MAX_SINGLE_OPERATION_SIZE_BYTES)[0]

    assert operation.operation_type == OperationType.CREATE_RUN
    assert operation.sequence_id == 1
    assert operation.operation == run


def test_get_operations(operations_repo):
    # Given
    snapshots = []
    for i in range(5):
        snapshot = UpdateRunSnapshot(assign={f"key_{i}": Value(string="a" * (1024 * 1024 * 2 - 100))})
        snapshots.append(snapshot)

    operations_repo.save_update_run_snapshots(snapshots)

    sizes = [i.ByteSize() for i in snapshots]

    # When - get operations up to a size that should include the first 2 operations
    request_size = sizes[0] + sizes[1]
    operations = operations_repo.get_operations(up_to_bytes=request_size)

    # Then
    assert len(operations) == 2
    assert [op.operation for op in operations] == snapshots[:2]
    assert all(op.operation_type == OperationType.UPDATE_SNAPSHOT for op in operations)
    assert [op.operation_size_bytes for op in operations] == [size for size in sizes[:2]]

    # When - get operations up to a (request size -1) - should return first operation only
    assert len(operations_repo.get_operations(up_to_bytes=request_size - 1)) == 1


def test_get_operations_size_based_pagination_with_many_items(operations_repo):
    # Given
    operations_count = 150_000
    snapshots = []
    for i in range(operations_count):
        snapshot = UpdateRunSnapshot(assign={f"key_{i}": Value(string=f"{i}" * 50)})
        snapshots.append(snapshot)

    operations_repo.save_update_run_snapshots(snapshots)

    sizes = [i.ByteSize() for i in snapshots]

    # when
    operations = operations_repo.get_operations(up_to_bytes=sum(sizes))

    # then
    assert len(operations) == operations_count

    # when
    operations = operations_repo.get_operations(up_to_bytes=sum(sizes[:10_000]))

    # then
    assert len(operations) == 10_000


@pytest.mark.parametrize("from_seq_id", [None, -1, 0, 1, 10_000, 50_000])
def test_get_operations_size_based_pagination_from_seq_id(operations_repo, from_seq_id):
    # Given
    operations_count = 150_000
    snapshots = []
    for i in range(operations_count):
        snapshot = UpdateRunSnapshot(assign={f"key_{i}": Value(string=f"{i}" * 50)})
        snapshots.append(snapshot)

    operations_repo.save_update_run_snapshots(snapshots)
    sizes = [i.ByteSize() for i in snapshots]
    start_index = max(from_seq_id or 0, 0)

    # when
    operations = operations_repo.get_operations(up_to_bytes=sum(sizes), from_exclusive=SequenceId(from_seq_id))

    # then
    assert len(operations) == operations_count - start_index

    # when
    operations = operations_repo.get_operations(
        up_to_bytes=sum(sizes[start_index:100_000]), from_exclusive=SequenceId(from_seq_id)
    )

    # then
    assert len(operations) == 100_000 - start_index


def test_get_operations_empty_db(operations_repo):
    # Given
    operations = operations_repo.get_operations(up_to_bytes=MAX_SINGLE_OPERATION_SIZE_BYTES)
    assert len(operations) == 0


def test_get_sequence_id_range_single(operations_repo):
    # Given
    snapshots = [UpdateRunSnapshot(assign={"key": Value(string="a")})]
    operations_repo.save_update_run_snapshots(snapshots)

    # When
    start_end = operations_repo.get_sequence_id_range()

    # Then
    assert start_end == (1, 1)


def test_get_sequence_id_range_multiple(operations_repo):
    # Given
    for i in range(5):
        snapshots = [UpdateRunSnapshot(assign={f"key_{i}": Value(string=f"value_{i}")})]
        operations_repo.save_update_run_snapshots(snapshots)

    # When
    start_end = operations_repo.get_sequence_id_range()

    # Then
    assert start_end == (1, 5)


def test_get_sequence_id_range_empty_db(operations_repo):
    # Given
    start_end = operations_repo.get_sequence_id_range()
    assert start_end is None


def test_delete_operations(operations_repo, temp_db_path):
    # Given
    snapshots = []
    for i in range(5):
        snapshot = UpdateRunSnapshot(assign={f"key_{i}": Value(string=f"value_{i}")})
        snapshots.append(snapshot)

    operations_repo.save_update_run_snapshots(snapshots)

    # Get the operations to find their sequence IDs
    operations = operations_repo.get_operations(up_to_bytes=MAX_SINGLE_OPERATION_SIZE_BYTES)
    assert len(operations) == 5

    # When - delete the first 3 operations
    deleted_count = operations_repo.delete_operations(up_to_seq_id=operations[2].sequence_id)

    # Then
    assert deleted_count == 3
    assert get_operation_count(temp_db_path) == 2


def test_delete_operations_invalid_id(operations_repo, temp_db_path):
    # Given
    snapshots = []
    for i in range(3):
        snapshot = UpdateRunSnapshot(assign={f"key_{i}": Value(string=f"value_{i}")})
        snapshots.append(snapshot)

    operations_repo.save_update_run_snapshots(snapshots)

    # When - try to delete with a non-positive sequence ID
    deleted_count = operations_repo.delete_operations(up_to_seq_id=SequenceId(0))

    # Then
    assert deleted_count == 0
    assert get_operation_count(temp_db_path) == 3


def test_save_and_get_metadata(operations_repo):
    # Given
    project = "test-project"
    run_id = "test-run-id"

    # When
    operations_repo.save_metadata(project=project, run_id=run_id)

    # Then
    metadata = operations_repo.get_metadata()
    assert metadata is not None

    expected_metadata = Metadata(project=project, run_id=run_id)
    assert expected_metadata == metadata


def test_get_metadata_nonexistent(operations_repo):
    # When
    metadata = operations_repo.get_metadata()

    # Then
    assert metadata is None


def test_metadata_already_exists_error(operations_repo):
    operations_repo.save_metadata(project="test", run_id="test")

    with pytest.raises(RuntimeError, match="Metadata already exists"):
        operations_repo.save_metadata(project="test2", run_id="test2")


def test_metadata_unsupported_version_error(temp_db_path, operations_repo):
    conn = sqlite3.connect(temp_db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO metadata (version, project, run_id)
        VALUES (?, ?, ?)
        """,
        ("wrong", "test1", "test1"),
    )
    conn.commit()
    conn.close()

    with pytest.raises(NeptuneLocalStorageInUnsupportedVersion):
        operations_repo.get_metadata()


def test_close_connection(operations_repo):
    # Given
    connection = operations_repo._connection
    assert connection is not None

    # When
    operations_repo.close(cleanup_files=True)

    # Then
    assert operations_repo._connection is None


@mock.patch("time.time")
def test_timestamp_in_operations(mock_time, operations_repo):
    # Given
    mock_time.return_value = 1234.567  # Fixed timestamp

    # When
    snapshot = UpdateRunSnapshot(assign={"key": Value(string="value")})
    operations_repo.save_update_run_snapshots([snapshot])

    # Then - Connect directly to the database to verify the timestamp
    conn = sqlite3.connect(operations_repo._db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT timestamp FROM run_operations")
    timestamp = cursor.fetchone()[0]
    conn.close()

    # Expected timestamp in milliseconds
    expected_timestamp = int(1234.567 * 1000)
    assert timestamp == expected_timestamp


def test_get_operations_up_to_bytes_too_small(operations_repo):
    with pytest.raises(RuntimeError, match=r"up to bytes is too small: 100 bytes.*"):
        operations_repo.get_operations(up_to_bytes=100)


def test_save_update_run_snapshots_too_large(operations_repo):
    with pytest.raises(RuntimeError, match=r"Operation size \(2097172\) exceeds the limit of 2097152 bytes"):
        operations_repo.save_update_run_snapshots(
            [UpdateRunSnapshot(assign={"key": Value(string="a" * 1024 * 1024 * 2)})]
        )


def get_operation_count(db_path: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM run_operations")
        count: int = cursor.fetchone()[0]
        return count
    finally:
        conn.close()


def test_save_file_upload_requests(operations_repo, temp_db_path):
    # Given
    file_requests = [
        FileUploadRequest(
            source_path=f"source_path_{i}",
            destination=f"destination_{i}",
            mime_type="application/octet-stream",
            size_bytes=i * 1024,
            is_temporary=bool(i % 2),
        )
        for i in range(3)
    ]

    # When
    last_id = operations_repo.save_file_upload_requests(file_requests)

    # Then
    assert last_id == 3

    # Then
    conn = sqlite3.connect(operations_repo._db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT id, source_path, destination, mime_type, size_bytes, is_temporary FROM file_upload_requests")
    rows = cursor.fetchall()
    conn.close()
    assert rows == [
        (i + 1, request.source_path, request.destination, request.mime_type, request.size_bytes, request.is_temporary)
        for i, request in enumerate(file_requests)
    ]


def test_get_file_upload_requests_empty(operations_repo):
    # Given
    operations = operations_repo.get_file_upload_requests(n=10)
    assert len(operations) == 0


def test_get_file_upload_requests_nonempty(operations_repo):
    # Given
    file_requests = [
        FileUploadRequest(
            source_path=f"source_path_{i}",
            destination=f"destination_{i}",
            mime_type="application/octet-stream",
            size_bytes=i * 1024,
            is_temporary=bool(i % 2),
        )
        for i in range(3)
    ]
    operations_repo.save_file_upload_requests(file_requests)

    # When
    requests = operations_repo.get_file_upload_requests(n=10)

    # Then
    assert len(requests) == 3
    assert requests == [
        FileUploadRequest(**{**dataclasses.asdict(file_requests[i]), "sequence_id": SequenceId(i + 1)})
        for i in range(3)
    ]


def test_get_file_upload_requests_count_empty(operations_repo):
    # Given
    count = operations_repo.get_file_upload_requests_count()
    assert count == 0


def test_get_file_upload_requests_count_nonempty(operations_repo):
    # Given
    file_requests = [
        FileUploadRequest(
            source_path=f"source_path_{i}",
            destination=f"destination_{i}",
            mime_type="application/octet-stream",
            size_bytes=i * 1024,
            is_temporary=bool(i % 2),
        )
        for i in range(3)
    ]
    operations_repo.save_file_upload_requests(file_requests)

    # When
    count = operations_repo.get_file_upload_requests_count()

    # Then
    assert count == 3


def test_get_file_upload_requests_count_nonempty_limit(operations_repo):
    # Given
    file_requests = [
        FileUploadRequest(
            source_path=f"source_path_{i}",
            destination=f"destination_{i}",
            mime_type="application/octet-stream",
            size_bytes=i * 1024,
            is_temporary=bool(i % 2),
        )
        for i in range(3)
    ]
    operations_repo.save_file_upload_requests(file_requests)

    # When
    count = operations_repo.get_file_upload_requests_count(limit=2)

    # Then
    assert count == 2


def test_get_file_upload_requests_with_limit(operations_repo):
    # Given
    file_requests = [
        FileUploadRequest(
            source_path=f"source_path_{i}",
            destination=f"destination_{i}",
            mime_type="application/octet-stream",
            size_bytes=i * 1024,
            is_temporary=bool(i % 2),
        )
        for i in range(3)
    ]
    operations_repo.save_file_upload_requests(file_requests)

    # When
    requests = operations_repo.get_file_upload_requests(n=2)

    # Then
    assert len(requests) == 2
    assert requests == [
        FileUploadRequest(**{**dataclasses.asdict(file_requests[i]), "sequence_id": SequenceId(i + 1)})
        for i in range(2)
    ]


def test_delete_file_upload_requests(operations_repo):
    # Given
    file_requests = [
        FileUploadRequest(
            source_path=f"source_path_{i}",
            destination=f"destination_{i}",
            mime_type="application/octet-stream",
            size_bytes=i * 1024,
            is_temporary=bool(i % 2),
        )
        for i in range(3)
    ]
    operations_repo.save_file_upload_requests(file_requests)

    # When
    operations_repo.delete_file_upload_requests(seq_ids=[SequenceId(1), SequenceId(3)])

    # Then
    requests = operations_repo.get_file_upload_requests(n=10)
    assert len(requests) == 1
    assert requests == [FileUploadRequest(**{**dataclasses.asdict(file_requests[1]), "sequence_id": SequenceId(2)})]


def test_delete_file_upload_requests_invalid_id(operations_repo):
    # Given
    file_requests = [
        FileUploadRequest(
            source_path=f"source_path_{i}",
            destination=f"destination_{i}",
            mime_type="application/octet-stream",
            size_bytes=i * 1024,
            is_temporary=bool(i % 2),
        )
        for i in range(3)
    ]
    operations_repo.save_file_upload_requests(file_requests)

    # When - try to delete with a non-positive sequence ID
    operations_repo.delete_file_upload_requests(seq_ids=[SequenceId(0)])

    # Then
    requests = operations_repo.get_file_upload_requests(n=10)
    assert len(requests) == 3


def test_delete_file_upload_requests_empty(operations_repo):
    # Given
    file_requests = [
        FileUploadRequest(
            source_path=f"source_path_{i}",
            destination=f"destination_{i}",
            mime_type="application/octet-stream",
            size_bytes=i * 1024,
            is_temporary=bool(i % 2),
        )
        for i in range(3)
    ]
    operations_repo.save_file_upload_requests(file_requests)

    # When - try to delete with an empty list
    operations_repo.delete_file_upload_requests(seq_ids=[])

    # Then
    requests = operations_repo.get_file_upload_requests(n=10)
    assert len(requests) == 3


@pytest.mark.parametrize("cleanup_files", [True, False])
def test_cleanup_repository_empty(temp_db_path, cleanup_files):
    # given
    repo = OperationsRepository(db_path=Path(temp_db_path))
    assert not os.path.exists(temp_db_path)

    # when
    repo.init_db()

    # then
    assert os.path.exists(temp_db_path)

    # when
    repo.close(cleanup_files=cleanup_files)

    # then
    assert os.path.exists(temp_db_path) != cleanup_files


@pytest.mark.parametrize("cleanup_files", [True, False])
def test_cleanup_repository_no_tables(temp_db_path, cleanup_files):
    # given
    repo = OperationsRepository(db_path=Path(temp_db_path))

    # when
    repo.init_db()
    conn = sqlite3.connect(temp_db_path)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE run_operations")
    cursor.execute("DROP TABLE metadata")
    cursor.execute("DROP TABLE file_upload_requests")
    conn.commit()
    conn.close()

    # then
    assert os.path.exists(temp_db_path)

    # when
    repo.close(cleanup_files=cleanup_files)

    # then
    assert os.path.exists(temp_db_path) != cleanup_files


@pytest.mark.parametrize("cleanup_files", [True, False])
def test_cleanup_repository_nonempty_run_snapshots(temp_db_path, cleanup_files):
    # given
    repo = OperationsRepository(db_path=Path(temp_db_path))

    # when
    repo.init_db()
    repo.save_update_run_snapshots([UpdateRunSnapshot(assign={"key": Value(string="value")})])

    # then
    assert os.path.exists(temp_db_path)

    # when
    repo.close(cleanup_files=cleanup_files)

    # then
    assert os.path.exists(temp_db_path)


@pytest.mark.parametrize("cleanup_files", [True, False])
def test_cleanup_repository_nonempty_file_requests(temp_db_path, cleanup_files):
    # given
    repo = OperationsRepository(db_path=Path(temp_db_path))

    # when
    repo.init_db()
    repo.save_file_upload_requests(
        [
            FileUploadRequest(
                source_path=f"source_path_{i}",
                destination=f"destination_{i}",
                mime_type="application/octet-stream",
                size_bytes=i * 1024,
                is_temporary=bool(i % 2),
            )
            for i in range(3)
        ]
    )

    # then
    assert os.path.exists(temp_db_path)

    # when
    repo.close(cleanup_files=cleanup_files)

    # then
    assert os.path.exists(temp_db_path)


@pytest.mark.skip(reason="We do not support the case of two processes owning the same repository")
def test_cleanup_repository_conflict(temp_db_path):
    # given
    repo1 = OperationsRepository(db_path=Path(temp_db_path))
    repo2 = OperationsRepository(db_path=Path(temp_db_path))

    # when
    repo1.init_db()
    repo2.init_db()

    # then
    assert os.path.exists(temp_db_path)

    # when
    repo2.close(cleanup_files=True)

    # then
    assert os.path.exists(temp_db_path)

    # when
    repo1.close(cleanup_files=True)

    # then
    assert not os.path.exists(temp_db_path)


def test_cleanup_repository_resume(temp_db_path):
    # when
    repo1 = OperationsRepository(db_path=Path(temp_db_path))
    repo1.init_db()
    repo1.save_update_run_snapshots([UpdateRunSnapshot(assign={"key": Value(string="value")})])
    repo1.close(cleanup_files=True)

    # then
    assert os.path.exists(temp_db_path)

    # when
    repo2 = OperationsRepository(db_path=Path(temp_db_path))
    repo2.delete_operations(up_to_seq_id=SequenceId(1))
    repo2.close(cleanup_files=True)

    # then
    assert not os.path.exists(temp_db_path)


@pytest.mark.parametrize("log_failure_action", [None, "raise", "drop"])
def test_concurrent_save_update_run_snapshots(monkeypatch, temp_db_path, log_failure_action):
    if log_failure_action:
        monkeypatch.setenv(envs.LOG_FAILURE_ACTION, log_failure_action)
    else:
        monkeypatch.delenv(envs.LOG_FAILURE_ACTION, raising=False)

    operations_repo = OperationsRepository(db_path=Path(temp_db_path), timeout=1)
    operations_repo.init_db()

    with _concurrent_transaction_cursor(temp_db_path) as cursor:
        _write_run_operation(cursor)

        if log_failure_action == "raise":
            with pytest.raises(NeptuneUnableToLogData):
                operations_repo.save_update_run_snapshots([UpdateRunSnapshot(assign={"key": Value(string="value")})])
        else:
            operations_repo.save_update_run_snapshots([UpdateRunSnapshot(assign={"key": Value(string="value")})])

    operations_repo.close(cleanup_files=True)


@pytest.mark.parametrize(
    "save_operation",
    [
        lambda repo: repo.save_metadata(project="test", run_id="test"),
        lambda repo: repo.save_create_run(CreateRun(family="test-run-id", experiment_id="Test Run")),
    ],
)
def test_concurrent_saves_other(monkeypatch, temp_db_path, save_operation):
    operations_repo = OperationsRepository(db_path=Path(temp_db_path), timeout=1)
    operations_repo.init_db()

    with _concurrent_transaction_cursor(temp_db_path) as cursor:
        _write_run_operation(cursor)

        with pytest.raises(NeptuneUnableToLogData):
            save_operation(operations_repo)

    operations_repo.close(cleanup_files=True)


def test_concurrent_reads(temp_db_path):
    operations_repo = OperationsRepository(db_path=Path(temp_db_path), timeout=1)
    operations_repo.init_db()

    with _concurrent_transaction_cursor(temp_db_path) as cursor:
        _write_run_operation(cursor)

        operations_repo.get_operations(up_to_bytes=MAX_SINGLE_OPERATION_SIZE_BYTES)
        operations_repo.get_metadata()
        operations_repo.get_sequence_id_range()
    operations_repo.close(cleanup_files=True)


def test_concurrent_delete_sqlite_busy(temp_db_path):
    operations_repo = OperationsRepository(db_path=Path(temp_db_path), timeout=1)
    operations_repo.init_db()

    with _concurrent_transaction_cursor(temp_db_path) as cursor:
        _write_run_operation(cursor)

        with pytest.raises(NeptuneUnableToLogData) as exc:
            operations_repo.delete_operations(up_to_seq_id=SequenceId(1))
        assert "database is locked" in str(exc.value.__cause__)
    operations_repo.close(cleanup_files=True)


@contextlib.contextmanager
def _concurrent_transaction_cursor(temp_db_path):
    conn = sqlite3.connect(temp_db_path)
    conn.execute("BEGIN")
    cursor = conn.cursor()
    yield cursor
    cursor.close()
    conn.commit()
    conn.close()


def _write_run_operation(cursor):
    op = UpdateRunSnapshot(assign={"key": Value(string="value")})
    serialized_op = op.SerializeToString()
    cursor.executemany(
        """
        INSERT INTO run_operations (timestamp, operation_type, operation, operation_size_bytes)
        VALUES (?, ?, ?, ?)
        """,
        [(12345, OperationType.UPDATE_SNAPSHOT, serialized_op, len(serialized_op))],
    )
