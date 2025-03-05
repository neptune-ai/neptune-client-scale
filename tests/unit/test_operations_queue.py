import logging
import threading
import time
from time import monotonic

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


def test_negative_blocking_time(monkeypatch):
    monkeypatch.setenv(envs.LOG_MAX_BLOCKING_TIME_SECONDS, "-1")
    with pytest.raises(ValueError) as exc:
        OperationsQueue(lock=threading.RLock(), max_size=1)
    exc.match(f"{envs.LOG_MAX_BLOCKING_TIME_SECONDS}.* non-negative")


def test_invalid_blocking_time(monkeypatch):
    monkeypatch.setenv(envs.LOG_MAX_BLOCKING_TIME_SECONDS, "invalid")
    with pytest.raises(ValueError) as exc:
        OperationsQueue(lock=threading.RLock(), max_size=1)
    exc.match(f"{envs.LOG_MAX_BLOCKING_TIME_SECONDS}.* must be an integer")


def _get_delayed(queue):
    """Used in a thread to consume an element from the queue after a fixed delay"""
    time.sleep(1.0)
    queue.queue.get(timeout=0.5)


def test_enqueue_blocking_timing(monkeypatch):
    """Test the blocking behaviour of the queue when depending on whether it's full or empty.

    Note that when checking instead of checking if the elapsed time is >= 2.0,
    we apply some tolerance (check for > 1.9 instead), as the actual time blocking
    on the queue might vary slightly, in particular we can return tiny fractions of a second earlier
    """

    monkeypatch.setenv(envs.LOG_FAILURE_ACTION, "drop")
    monkeypatch.setenv(envs.LOG_MAX_BLOCKING_TIME_SECONDS, "2")

    queue = OperationsQueue(lock=threading.RLock(), max_size=1)

    # Queue empty: enqueue should not block
    t0 = monotonic()
    queue.enqueue(operation=RunOperation(), size=10)
    assert monotonic() - t0 < 0.5, "enqueue() on an empty queue should not block"

    # Queue full: block on first attempt to enqueue an item
    t0 = monotonic()
    queue.enqueue(operation=RunOperation(), size=10)
    assert monotonic() - t0 > 1.9, "enqueue() on a full queue should block"

    # Queue full: don't block on further failed attempts to enqueue items
    t0 = monotonic()
    for _ in range(5):
        queue.enqueue(operation=RunOperation(), size=10)
    assert monotonic() - t0 < 0.5, "enqueue() on a full queue should not block after the previous call failed"

    # Queue empty again: enqueue should not block
    queue.queue.get(timeout=0.5)
    t0 = monotonic()
    queue.enqueue(operation=RunOperation(), size=10)
    assert monotonic() - t0 < 0.5, "enqueue() on an empty queue should not block"

    # Start a thread that consumes an element from the queue while we're blocked on enqueue()
    thread = threading.Thread(target=_get_delayed, args=(queue,), daemon=True)
    thread.start()

    # Queue initially full, but is emptied during wait: enqueue should block but only until there is free space
    t0 = monotonic()
    queue.enqueue(operation=RunOperation(), size=10)
    elapsed = monotonic() - t0
    assert elapsed > 0.5, "enqueue() on a full queue should block"
    assert elapsed < 2.0, "Waiting on the queue should be interrupted once there is free capacity"

    # Queue full again: we should block
    t0 = monotonic()
    queue.enqueue(operation=RunOperation(), size=10)
    assert monotonic() - t0 > 1.9, "enqueue() on a full queue should block"

    thread.join()


def test_enqueue_drop_on_queue_full(monkeypatch, caplog):
    """Test the behaviour of enqueue() with the "drop" action on a full queue"""

    monkeypatch.setenv(envs.LOG_FAILURE_ACTION, "drop")
    monkeypatch.setenv(envs.LOG_MAX_BLOCKING_TIME_SECONDS, "2")
    caplog.set_level(logging.ERROR, logger="neptune")

    queue = OperationsQueue(lock=threading.RLock(), max_size=1)

    # Queue empty: enqueue must succeed
    queue.enqueue(operation=RunOperation(), size=10)
    assert not caplog.records, "enqueue() must succeed on an empty queue"

    # Queue full: drop subsequent items
    for _ in range(3):
        queue.enqueue(operation=RunOperation(), size=10)

    assert len(caplog.records) == 3, "An error should be logged for each failed enqueue()"
    for rec in caplog.records:
        assert "queue is full" in rec.message

    # Queue empty: enqueue must succeed
    queue.queue.get(timeout=0.5)
    caplog.clear()
    queue.enqueue(operation=RunOperation(), size=10)
    assert not caplog.records, "enqueue() must succeed on an empty queue"

    # Start a thread that consumes an element from the queue while we're blocked on enqueue()
    thread = threading.Thread(target=_get_delayed, args=(queue,))
    thread.start()

    # Queue initially full, but is emptied during wait: enqueue should succeed
    queue.enqueue(operation=RunOperation(), size=10)
    assert not caplog.records, "enqueue() must succeed when queue is emptied during wait"

    # Queue full: drop the item
    queue.enqueue(operation=RunOperation(), size=10)
    assert len(caplog.records) == 1, "a single error should be logged"
    assert "queue is full" in caplog.text

    thread.join()


def test_enqueue_raise_on_queue_full(monkeypatch):
    """Test the behaviour of enqueue() with the "drop" action on a full queue"""

    monkeypatch.setenv(envs.LOG_FAILURE_ACTION, "raise")
    monkeypatch.setenv(envs.LOG_MAX_BLOCKING_TIME_SECONDS, "2")

    queue = OperationsQueue(lock=threading.RLock(), max_size=1)

    # Queue empty: enqueue must succeed
    queue.enqueue(operation=RunOperation(), size=10)

    # Queue full: drop subsequent items
    for x in range(3):
        with pytest.raises(NeptuneUnableToLogData) as exc:
            queue.enqueue(operation=RunOperation(), size=10)
        exc.match("queue is full")

    # Queue empty: enqueue must succeed
    queue.queue.get(timeout=0.5)
    queue.enqueue(operation=RunOperation(), size=10)

    # Start a thread that consumes an element from the queue while we're blocked on enqueue()
    thread = threading.Thread(target=_get_delayed, args=(queue,))
    thread.start()

    # Queue initially full, but is emptied during wait: enqueue() should succeed
    queue.enqueue(operation=RunOperation(), size=10)

    # Queue full: drop the item
    with pytest.raises(NeptuneUnableToLogData) as exc:
        queue.enqueue(operation=RunOperation(), size=10)
    exc.match("queue is full")

    thread.join()
