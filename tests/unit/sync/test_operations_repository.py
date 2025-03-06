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

from neptune_scale.sync.operations_repository import (
    Metadata,
    NeptuneLocalStorageInUnsupportedVersion,
    OperationsRepository,
    OperationType,
)
from neptune_scale.sync.parameters import MAX_SINGLE_OPERATION_SIZE_BYTES


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
    repo.close()


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
    repo.close()


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
    operations_repo.save_update_run_snapshots(snapshots)

    # Then
    count = get_operation_count(temp_db_path)
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

    # When
    operations = operations_repo.get_operations(up_to_bytes=sum(sizes))
    assert len(operations) == operations_count

    operations = operations_repo.get_operations(up_to_bytes=sum(sizes[:10_000]))
    assert len(operations) == 10_000


def test_get_operations_empty_db(operations_repo):
    # Given
    operations = operations_repo.get_operations(up_to_bytes=MAX_SINGLE_OPERATION_SIZE_BYTES)
    assert len(operations) == 0


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
    deleted_count = operations_repo.delete_operations(up_to_seq_id=0)

    # Then
    assert deleted_count == 0
    assert get_operation_count(temp_db_path) == 3


def test_save_and_get_metadata(operations_repo):
    # Given
    project = "test-project"
    run_id = "test-run-id"
    parent_run_id = "parent-run-id"
    fork_step = 1.5

    # When
    operations_repo.save_metadata(project=project, run_id=run_id, parent_run_id=parent_run_id, fork_step=fork_step)

    # Then
    metadata = operations_repo.get_metadata()
    assert metadata is not None

    expected_metadata = Metadata(
        version="v1", project=project, run_id=run_id, parent_run_id=parent_run_id, fork_step=fork_step
    )
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


def test_close_connection(operations_repo):
    # Given
    connection = operations_repo._connection
    assert connection is not None

    # When
    operations_repo.close()

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
    with pytest.raises(RuntimeError, match="Operation size is too large: 2097172 bytes"):
        operations_repo.save_update_run_snapshots(
            [UpdateRunSnapshot(assign={"key": Value(string="a" * 1024 * 1024 * 2)})]
        )


def test_unsupported_version_error(operations_repo, temp_db_path):
    # Given - create a database with an unsupported version
    operations_repo.init_db()
    operations_repo.save_metadata(project="test", run_id="test")

    conn = sqlite3.connect(temp_db_path)
    conn.execute("UPDATE metadata SET version = 'unsupported_version'")
    conn.commit()
    conn.close()

    # When/Then - trying to initialize with an existing DB with wrong version should raise an error
    with pytest.raises(NeptuneLocalStorageInUnsupportedVersion):
        operations_repo = OperationsRepository(db_path=Path(temp_db_path))
        operations_repo.init_db()
        operations_repo.get_metadata()


def get_operation_count(db_path: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM run_operations")
        count: int = cursor.fetchone()[0]
        return count
    finally:
        conn.close()
