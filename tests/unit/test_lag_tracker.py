import time
from threading import Event
from unittest.mock import Mock

from freezegun import freeze_time

from neptune_scale.sync.lag_tracking import LagTracker
from neptune_scale.util import SharedFloat


@freeze_time("2024-09-01 00:00:00")
def test__lag_tracker__callback_called():
    # given
    lag = 5.0
    async_lag_threshold = 1.0

    # and
    errors_queue = Mock()
    operations_queue = Mock(last_timestamp=time.time())
    last_ack_timestamp = SharedFloat(time.time() - lag)
    callback = Mock()

    # Synchronization event
    callback_called = Event()

    # Modify the callback to set the event when called
    def callback_with_event() -> None:
        callback()
        callback_called.set()

    # and
    lag_tracker = LagTracker(
        errors_queue=errors_queue,
        operations_queue=operations_queue,
        last_ack_timestamp=last_ack_timestamp,
        async_lag_threshold=async_lag_threshold,
        on_async_lag_callback=callback_with_event,
    )
    lag_tracker.start()

    # when
    lag_tracker.wake_up()

    # then
    assert callback_called.wait(timeout=5), "Callback was not called within the timeout"

    # and - cleanup
    lag_tracker.interrupt()
    lag_tracker.join(timeout=5)

    # then
    callback.assert_called()


@freeze_time("2024-09-01 00:00:00")
def test__lag_tracker__not_called():
    # given
    lag = 5.0
    async_lag_threshold = 10.0

    # and
    errors_queue = Mock()
    operations_queue = Mock(last_timestamp=time.time())
    last_ack_timestamp = SharedFloat(time.time() - lag)
    callback = Mock()

    # and
    lag_tracker = LagTracker(
        errors_queue=errors_queue,
        operations_queue=operations_queue,
        last_ack_timestamp=last_ack_timestamp,
        async_lag_threshold=async_lag_threshold,
        on_async_lag_callback=callback,
    )
    lag_tracker.start()

    # when
    lag_tracker.wake_up()

    # then
    callback.assert_not_called()

    # and - cleanup
    lag_tracker.interrupt()
    lag_tracker.join(timeout=5)
