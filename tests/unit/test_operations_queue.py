import logging
import multiprocessing
import threading
import time

import pytest
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
    UpdateRunSnapshot,
    Value,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale.exceptions import NeptuneUnableToLogData
from neptune_scale.sync.operations_queue import OperationsQueue
from neptune_scale.util import envs


def test__enqueue():
    # given
    lock = threading.RLock()
    queue = OperationsQueue(lock=lock, max_size=0)

    # and
    operation = RunOperation()

    # when
    queue.enqueue(operation=operation, size=0)

    # then
    assert queue._sequence_id == 1

    # when
    queue.enqueue(operation=operation, size=0)

    # then
    assert queue._sequence_id == 2


def test_drop_on_max_element_size_exceeded(monkeypatch, caplog):
    monkeypatch.setenv(envs.LOG_FAILURE_ACTION, "drop")

    # given
    lock = threading.RLock()
    queue = OperationsQueue(lock=lock, max_size=1)

    # and
    snapshot = UpdateRunSnapshot(assign={f"key_{i}": Value(string=("a" * 1024)) for i in range(1024)})
    operation = RunOperation(update=snapshot)

    # then
    with caplog.at_level(logging.ERROR, logger="neptune"):
        queue.enqueue(operation=operation, size=snapshot.ByteSize())
    assert len(caplog.records) == 1
    assert "Operation size exceeds the maximum allowed size" in caplog.text


def test_raise_on_max_element_size_exceeded(monkeypatch, caplog):
    monkeypatch.setenv(envs.LOG_FAILURE_ACTION, "raise")

    lock = threading.RLock()
    queue = OperationsQueue(lock=lock, max_size=1)

    snapshot = UpdateRunSnapshot(assign={f"key_{i}": Value(string=("a" * 1024)) for i in range(1024)})
    operation = RunOperation(update=snapshot)

    with caplog.at_level(logging.ERROR, logger="neptune"), pytest.raises(NeptuneUnableToLogData) as exc:
        queue.enqueue(operation=operation, size=snapshot.ByteSize())

    assert not caplog.records, "No errors should be logged"
    assert exc.match("Operation size exceeds the maximum allowed size")


def test_invalid_log_failure_action(monkeypatch):
    monkeypatch.setenv(envs.LOG_FAILURE_ACTION, "invalid")
    with pytest.raises(ValueError) as exc:
        OperationsQueue(lock=threading.RLock(), max_size=1)
    exc.match(envs.LOG_FAILURE_ACTION)


def test_enqueue_block_and_drop_on_queue_full(monkeypatch, caplog):
    """
    Test the blocking behaviour of enqueue() with the "drop" action on a full queue:

     * we should not block when the full is not full
     * we should block and fail on the first call when the queue is full
     * we should fail without blocking on subsequent calls after the first one that blocked
     * an error should be emitted every time we drop data
    """

    monkeypatch.setenv(envs.LOG_FAILURE_ACTION, "drop")
    monkeypatch.setenv(envs.FREE_QUEUE_SLOT_TIMEOUT_SECS, "1")

    queue = OperationsQueue(lock=threading.RLock(), max_size=1)

    # First item must succeed and not block
    with caplog.at_level(logging.ERROR, logger="neptune"):
        t0 = time.monotonic()
        queue.enqueue(operation=RunOperation(), size=10, key="key")

    assert not caplog.records, "No error should be logged"
    assert time.monotonic() - t0 < 1.0, "First enqueue should not block"

    # Second item should wait for the specified timeout before giving up and logging an error
    with caplog.at_level(logging.ERROR, logger="neptune"):
        t0 = time.monotonic()
        queue.enqueue(operation=RunOperation(), size=10, key="key")

    assert time.monotonic() - t0 >= 1.0, "Second enqueue should block"
    assert len(caplog.records) == 1, "Only one error should be logged"
    assert "queue is full" in caplog.text

    # Further items should fail without blocking, each logging an error
    caplog.clear()
    with caplog.at_level(logging.ERROR, logger="neptune"):
        for i in range(5):
            t0 = time.monotonic()
            queue.enqueue(operation=RunOperation(), size=10, key="key")
            assert time.monotonic() - t0 < 1.0, f"Enqueue of item {i} should not block"

    assert len(caplog.records) == 5, "An error should be logged for each failed enqueue"
    for rec in caplog.records:
        assert "queue is full" in rec.message


def test_block_and_raise_on_queue_full(monkeypatch, caplog):
    """
    Test the blocking behaviour of enqueue() with the "raise" action on a full queue:

     * we should not block when the full is not full
     * we should block and fail on the first call when the queue is full
     * we should fail without blocking on subsequent calls after the first one that blocked
     * NeptuneUnableToLogData should be raised every time we drop data
    """

    monkeypatch.setenv(envs.LOG_FAILURE_ACTION, "raise")
    monkeypatch.setenv(envs.FREE_QUEUE_SLOT_TIMEOUT_SECS, "1")

    queue = OperationsQueue(lock=threading.RLock(), max_size=1)

    # First item must succeed and not block
    with caplog.at_level(logging.ERROR, logger="neptune"):
        t0 = time.monotonic()
        queue.enqueue(operation=RunOperation(), size=10, key="key")

    assert not caplog.records, "No errors should be logged"
    assert time.monotonic() - t0 < 1.0, "First enqueue should not block"

    # Second item should wait for the specified timeout before giving up and
    # raising an exception, without logging an error
    with caplog.at_level(logging.ERROR, logger="neptune"), pytest.raises(NeptuneUnableToLogData) as exc:
        t0 = time.monotonic()
        queue.enqueue(operation=RunOperation(), size=10, key="key")

    assert time.monotonic() - t0 >= 1.0, "Second enqueue should block"
    assert not caplog.records, "No errors should be logged"
    exc.match("queue is full")

    # Further items should fail without blocking, each raising an exception and logging no errors
    with caplog.at_level(logging.ERROR, logger="neptune"):
        for i in range(5):
            t0 = time.monotonic()
            with pytest.raises(NeptuneUnableToLogData) as exc:
                queue.enqueue(operation=RunOperation(), size=10, key="key")

            assert time.monotonic() - t0 < 1.0, f"Enqueue of item {i} should not block"
            exc.match("queue is full")

    assert not caplog.records, "No errors should be logged"


def test_unblocking_queue_and_locking_again(monkeypatch):
    """
    Test a scenario, in which the queue is full for some time, but then gets unlocked.
    We should only block on the first enqueue() after the queue is full, and not on subsequent ones.

    Steps:
    1. Saturate the queue.
    2. Make sure we only block the first call to enqueue() on a full queue.
    3. Free up the queue. Repeat steps 1-3 a few times.
    4. Saturate the queue again. This call should succeed as the queue is empty.
    5. Block on the first enqueue() after the previous one, but free up the queue during the wait.
       This call to enqueue() should succeed, with blocking that is interrupted.
    6. Free up the queue. The next enqueue() should succeed without blocking, because we've had success in step 5.
    7. The next enqueue() should block again, and fail.
    8. Subsequent calls to enqueue() should fail without blocking
    """

    monkeypatch.setenv(envs.LOG_FAILURE_ACTION, "raise")
    monkeypatch.setenv(envs.FREE_QUEUE_SLOT_TIMEOUT_SECS, "2")

    queue = OperationsQueue(lock=threading.RLock(), max_size=1)

    for step in range(3):
        t0 = time.monotonic()
        # Step 1: Saturate the queue.
        queue.enqueue(operation=RunOperation(), size=10, key="key")
        assert time.monotonic() - t0 < 2, f"{step=}: First enqueue should not block"

        # Step 2: Make sure we only block the first call to enqueue() on a full queue.
        with pytest.raises(NeptuneUnableToLogData) as exc:
            t0 = time.monotonic()
            queue.enqueue(operation=RunOperation(), size=10, key="key")
        assert time.monotonic() - t0 >= 2, f"{step=}: Second enqueue should block"
        exc.match("queue is full")

        for i in range(3, 10):
            t0 = time.monotonic()
            with pytest.raises(NeptuneUnableToLogData) as exc:
                queue.enqueue(operation=RunOperation(), size=10, key="key")

            assert (
                time.monotonic() - t0 < 2
            ), f"{step=}, enqueue on a full queue should not block if the previous call failed"
            exc.match("queue is full")

        # Step 3: Free up the queue and repeat the process
        # Note: We use get with a small timeout to give the queue's internal feeder thread some time to actually
        # send the message otherwise we could get Empty.
        queue.queue.get(timeout=0.5)

    # Step 4: Saturate the queue again. This call should succeed as the queue is empty
    queue.enqueue(operation=RunOperation(), size=10, key="key")

    # Used to wait until the process launches
    event = multiprocessing.Event()
    process = multiprocessing.Process(target=_get_delayed, args=(event, queue.queue))
    process.start()
    assert event.wait(timeout=30), "Failed to start child process in time"

    # Step 5: We should be unblocked midway through the wait because of the child process reading data
    # from the queue, so no exception should be raised
    t0 = time.monotonic()
    queue.enqueue(operation=RunOperation(), size=10, key="key")
    elapsed = time.monotonic() - t0
    assert elapsed >= 1.0, "The queue did not block"
    assert elapsed < 2.0, "Waiting on the queue should be interrupted once there is free capacity"

    process.join()

    # Step 6: Free up the queue and make sure we don't block on the first enqueue() call after
    # the previous one which was a success.
    # Note: We use get with a small timeout to give the queue's internal feeder thread some time to actually
    # send the message otherwise we could get Empty.
    queue.queue.get(timeout=0.5)

    t0 = time.monotonic()
    queue.enqueue(operation=RunOperation(), size=10, key="key")

    assert (
        time.monotonic() - t0 < 2.0
    ), "After a successful call to enqueue the next call should not block if the queue is not full"

    # Step 7: The first enqueue() to a full queue should always block, if the previous call succeeded
    # block.
    with pytest.raises(NeptuneUnableToLogData) as exc:
        t0 = time.monotonic()
        queue.enqueue(operation=RunOperation(), size=10, key="key")

    exc.match("queue is full")
    assert (
        time.monotonic() - t0 >= 2
    ), "After a successful call to enqueue the next call should block if the queue is full"

    # Step 8: Subsequent calls to enqueue() should fail without blocking, because the queue is full and the previous
    # call already blocked and failed.
    for i in range(3, 10):
        t0 = time.monotonic()
        with pytest.raises(NeptuneUnableToLogData) as exc:
            queue.enqueue(operation=RunOperation(), size=10, key="key")

        assert time.monotonic() - t0 < 2, f"{i=}, enqueue on a full queue should not block if the previous call failed"
        exc.match("queue is full")


def _get_delayed(event: multiprocessing.Event, queue: multiprocessing.Queue):
    event.set()
    time.sleep(1.0)
    queue.get_nowait()
