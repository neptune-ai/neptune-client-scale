import multiprocessing
import time
from datetime import datetime
from threading import Event
from unittest.mock import Mock

from neptune_scale.sync.lag_tracking import LagTracker

mp_context = multiprocessing.get_context("spawn")


def test__lag_tracker__callback_called():
    # given
    async_lag_threshold = 1.0

    # and
    operations_repository = Mock()
    operations_repository.get_operations_min_timestamp.return_value = datetime.now()
    callback = Mock()

    # Synchronization event
    callback_called = Event()

    # Modify the callback to set the event when called
    def callback_with_event() -> None:
        callback()
        callback_called.set()

    # and
    lag_tracker = LagTracker(
        operations_repository=operations_repository,
        async_lag_threshold=async_lag_threshold,
        on_async_lag_callback=callback_with_event,
    )
    lag_tracker.start()

    # then
    assert callback_called.wait(timeout=5), "Callback was not called within the timeout"

    # and - cleanup
    lag_tracker.interrupt()
    lag_tracker.join(timeout=5)

    # then
    callback.assert_called()


def test__lag_tracker__not_called():
    # given
    async_lag_threshold = 10.0

    # and
    operations_repository = Mock()
    operations_repository.get_operations_min_timestamp.return_value = datetime.now()
    callback = Mock()

    # and
    lag_tracker = LagTracker(
        operations_repository=operations_repository,
        async_lag_threshold=async_lag_threshold,
        on_async_lag_callback=callback,
    )
    lag_tracker.start()

    # when
    time.sleep(2.0)

    # then
    callback.assert_not_called()

    # and - cleanup
    lag_tracker.interrupt()
    lag_tracker.join(timeout=5)
