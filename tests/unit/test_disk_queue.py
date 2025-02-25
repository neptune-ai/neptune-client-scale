import os
import tempfile
import threading

import pytest
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale.sync.disk_queue import SQLiteDiskQueue


@pytest.fixture
def temp_db_path():
    """Create a temporary database file path."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, "test_queue.db")
        yield db_path


@pytest.fixture
def disk_queue(temp_db_path):
    """Create a SQLiteDiskQueue instance with a temporary database."""
    queue = SQLiteDiskQueue(db_path=temp_db_path)
    yield queue
    queue.close()


@pytest.fixture
def sample_operation():
    """Create a sample RunOperation for testing."""
    return RunOperation(project="project", run_id="run_id").SerializeToString()


def test_init(temp_db_path):
    """Test initialization of SQLiteDiskQueue."""
    # when
    queue = SQLiteDiskQueue(db_path=temp_db_path)

    # then
    assert queue._db_path == temp_db_path
    assert queue._connection is not None
    assert os.path.exists(temp_db_path)

    # cleanup
    queue.close()


def test_enqueue_multiple_dequeue_multiple(disk_queue, sample_operation):
    """Test enqueuing and dequeuing multiple operations."""
    # when
    for _ in range(5):
        disk_queue.enqueue(sample_operation)

    # then
    assert disk_queue.get_operation_count() == 5

    # when
    results = disk_queue.peek(limit=3)
    disk_queue.delete_operations([result[0] for result in results])

    # then
    assert len(results) == 3
    assert disk_queue.get_operation_count() == 2

    # when
    results = disk_queue.peek(limit=3)
    disk_queue.delete_operations([result[0] for result in results])

    # then
    assert len(results) == 2
    assert disk_queue.get_operation_count() == 0


def test_peek(disk_queue, sample_operation):
    """Test peeking at operations without removing them."""
    # when
    for _ in range(3):
        disk_queue.enqueue(sample_operation)

    # then
    assert disk_queue.get_operation_count() == 3

    # when
    results = disk_queue.peek(limit=2)

    # then
    assert len(results) == 2
    assert disk_queue.get_operation_count() == 3  # Count should remain the same


def test_delete_operations(disk_queue, sample_operation):
    """Test deleting multiple operations at once."""
    # when
    for _ in range(5):
        disk_queue.enqueue(sample_operation)

    # then
    assert disk_queue.get_operation_count() == 5

    # when - delete a subset of operations
    deleted_count = disk_queue.delete_operations([1, 3])

    # then
    assert deleted_count == 2
    assert disk_queue.get_operation_count() == 3

    # when - delete with some non-existent IDs
    deleted_count = disk_queue.delete_operations([2, 999, 1000])

    # then
    assert deleted_count == 1
    assert disk_queue.get_operation_count() == 2

    # when - delete with empty list
    deleted_count = disk_queue.delete_operations([])

    # then
    assert deleted_count == 0
    assert disk_queue.get_operation_count() == 2


def test_get_operation_count(disk_queue, sample_operation):
    """Test getting the operation count."""
    # when - empty queue
    count = disk_queue.get_operation_count()

    # then
    assert count == 0

    # when - add some operations
    for _ in range(3):
        disk_queue.enqueue(sample_operation)

    # then
    assert disk_queue.get_operation_count() == 3


def test_close_and_reopen(temp_db_path, sample_operation):
    """Test closing and reopening the queue."""
    # given
    queue = SQLiteDiskQueue(db_path=temp_db_path)

    # when
    queue.enqueue(sample_operation)
    queue.close()

    # then
    assert queue._connection is None

    # when - reopen the queue
    queue = SQLiteDiskQueue(db_path=temp_db_path)

    # then - data should persist
    assert queue.get_operation_count() == 1
    results = queue.peek()
    assert len(results) == 1
    assert results[0][0] == 1

    # cleanup
    queue.close()


def test_concurrent_access(temp_db_path, sample_operation):
    """Test concurrent access to the queue."""

    # given
    queue = SQLiteDiskQueue(db_path=temp_db_path)
    num_operations = 100

    # when - enqueue operations from multiple threads
    def enqueue_batch():
        for _ in range(num_operations // 10):
            queue.enqueue(sample_operation)

    threads = [threading.Thread(target=enqueue_batch) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # then
    assert queue.get_operation_count() == num_operations

    # cleanup
    queue.close()


def test_enqueue_many(disk_queue, sample_operation):
    """Test enqueuing multiple operations at once using enqueue_many."""
    # given
    operations = [sample_operation for _ in range(5)]

    # when
    disk_queue.enqueue_many(operations)

    # then
    assert disk_queue.get_operation_count() == 5

    # when - peek all operations
    results = disk_queue.peek(limit=10)

    # then
    assert len(results) == 5
    # All operations should be the same as our sample
    for _, operation in results:
        assert operation == sample_operation

    # when - add more operations with enqueue_many
    more_operations = [sample_operation for _ in range(3)]
    disk_queue.enqueue_many(more_operations)

    # then
    assert disk_queue.get_operation_count() == 8


def test_enqueue_large_element(disk_queue):
    """Test enqueuing a large element (1MB) to ensure the queue can handle large payloads."""
    # given
    # Create a 1MB payload (1024 * 1024 bytes)
    large_payload = b"x" * (1024 * 1024)

    # when
    disk_queue.enqueue(large_payload)

    # then
    assert disk_queue.get_operation_count() == 1

    # when - retrieve the element
    results = disk_queue.peek(limit=1)

    # then
    assert len(results) == 1
    operation_id, retrieved_payload = results[0]
    assert len(retrieved_payload) == 1024 * 1024
    assert retrieved_payload == large_payload

    # when - enqueue multiple large elements
    large_elements = [large_payload for _ in range(3)]
    disk_queue.enqueue_many(large_elements)

    # then
    assert disk_queue.get_operation_count() == 4

    # when - retrieve all elements
    all_results = disk_queue.peek(limit=10)

    # then
    assert len(all_results) == 4
    # Verify all payloads are intact
    for _, payload in all_results:
        assert len(payload) == 1024 * 1024
        assert payload == large_payload
