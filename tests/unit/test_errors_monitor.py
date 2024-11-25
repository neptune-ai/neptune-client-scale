from threading import Event
from typing import Optional
from unittest.mock import Mock

from neptune_scale.sync.errors_tracking import (
    ErrorsMonitor,
    ErrorsQueue,
)


def test_errors_monitor():
    # given
    callback = Mock()

    # Synchronization event
    callback_called = Event()

    # Modify the callback to set the event when called
    def callback_with_event(exception: BaseException, last_called: Optional[float]) -> None:
        callback(exception, last_called)
        callback_called.set()

    # and
    errors_queue = ErrorsQueue()
    errors_monitor = ErrorsMonitor(errors_queue=errors_queue, on_error_callback=callback_with_event)
    errors_monitor.start()

    # when
    errors_queue.put(ValueError("error1"))
    errors_queue.flush()
    errors_monitor.wake_up()

    # then
    assert callback_called.wait(timeout=5), "Callback was not called within the timeout"

    # and - cleanup
    errors_monitor.interrupt()
    errors_monitor.join(timeout=5)

    # then
    callback.assert_called()
