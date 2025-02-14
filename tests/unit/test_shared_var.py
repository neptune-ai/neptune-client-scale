import time
from multiprocessing import Process

import pytest

from neptune_scale.util import (
    SharedFloat,
    SharedInt,
)


def _child_test_set_and_notify_both_ways(var):
    with var:
        # Ping
        var.value = 1
        var.notify_all()

    # Wait for Pong
    assert var.wait(timeout=10)
    assert var.value == 2


@pytest.mark.parametrize("tp", (SharedInt, SharedFloat))
def test_set_and_notify_both_ways(tp):
    """Test setting and notifying a variable from the parent and child process, both ways"""
    var = tp(0)

    process = Process(target=_child_test_set_and_notify_both_ways, args=(var,))
    process.start()

    # Wait for Ping
    assert var.wait(timeout=10)
    assert var.value == 1

    # Pong
    with var:
        var.value = 2
        var.notify_all()

    process.join()

    assert var.value == 2
    assert process.exitcode == 0, "Child process failed"


def _child_test_wait_notify_timeout(var, sleep_time):
    time.sleep(sleep_time)
    with var:
        var.value = 42
        var.notify_all()


@pytest.mark.parametrize("tp", (SharedInt, SharedFloat))
def test_wait_notify_before_timeout(tp):
    """Notify the variable before timeout is reached."""
    var = tp(0)

    process = Process(target=_child_test_wait_notify_timeout, args=(var, 2))
    process.start()

    t0 = time.monotonic()
    assert var.wait(timeout=10)

    elapsed = time.monotonic() - t0
    assert abs(elapsed - 2) < 0.5  # 0.5 second of tolerance to account for slow CI/CD workers
    assert elapsed < 10

    process.join()

    assert process.exitcode == 0, "Child process failed"
    assert var.value == 42


@pytest.mark.parametrize("tp", (SharedInt, SharedFloat))
def test_wait_notify_after_timeout(tp):
    """Notify the variable after timeout is reached."""

    var = tp(0)

    process = Process(target=_child_test_wait_notify_timeout, args=(var, 3))
    process.start()

    t0 = time.monotonic()
    assert not var.wait(timeout=1.5)

    elapsed = time.monotonic() - t0
    assert elapsed >= 1.5

    process.join()

    assert process.exitcode == 0, "Child process failed"
    assert var.value == 42
