from threading import Event
from unittest.mock import Mock

import pytest

from neptune_scale.exceptions import (
    NeptuneAsyncLagThresholdExceeded,
    NeptuneConnectionLostError,
    NeptuneOperationsQueueMaxSizeExceeded,
    NeptuneRetryableError,
    NeptuneScaleError,
    NeptuneScaleWarning,
    NeptuneSeriesPointDuplicate,
    NeptuneTooManyRequestsResponseError,
)
from neptune_scale.sync.errors_tracking import (
    ErrorsMonitor,
    ErrorsQueue,
)


@pytest.mark.parametrize(
    ["error", "callback_name"],
    [
        (NeptuneScaleError("error1"), "on_error_callback"),
        (NeptuneRetryableError("error1"), "on_warning_callback"),
        (ValueError("error2"), "on_error_callback"),
        (NeptuneScaleWarning("error3"), "on_warning_callback"),
        (NeptuneSeriesPointDuplicate("error4"), "on_warning_callback"),
        (NeptuneOperationsQueueMaxSizeExceeded("error5"), "on_queue_full_callback"),
        (NeptuneConnectionLostError("error6"), "on_network_error_callback"),
        (NeptuneAsyncLagThresholdExceeded("error7"), "on_async_lag_callback"),
        (NeptuneTooManyRequestsResponseError(), "on_warning_callback"),
    ],
)
def test_errors_monitor_callbacks_called(error, callback_name):
    # given
    callback = Mock()

    # Synchronization event
    callback_called = Event()

    # Modify the callback to set the event when called
    def callback_with_event(*args, **kwargs) -> None:
        callback()
        callback_called.set()

    # and
    errors_queue = ErrorsQueue()
    errors_monitor = ErrorsMonitor(**{"errors_queue": errors_queue, callback_name: callback_with_event})
    errors_monitor.start()

    # when
    errors_queue.put(error)
    errors_queue.flush()
    errors_monitor.wake_up()

    # then
    assert callback_called.wait(timeout=5), "Callback was not called within the timeout"

    # and - cleanup
    errors_monitor.interrupt()
    errors_monitor.join(timeout=5)

    # then
    callback.assert_called()
