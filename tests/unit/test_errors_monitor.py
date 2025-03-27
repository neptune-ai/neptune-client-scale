from threading import Event
from unittest.mock import Mock

import pytest

from neptune_scale.exceptions import (
    NeptuneAsyncLagThresholdExceeded,
    NeptuneConnectionLostError,
    NeptuneRetryableError,
    NeptuneScaleError,
    NeptuneScaleWarning,
    NeptuneSeriesPointDuplicate,
    NeptuneTooManyRequestsResponseError,
)
from neptune_scale.sync.errors_tracking import (
    CustomErrorsHandler,
    ErrorsMonitor,
    ErrorsQueue,
    RemoteErrorsHandler,
    RemoteErrorsHandlerAction,
)


@pytest.mark.parametrize(
    ["error", "callback_name"],
    [
        (NeptuneScaleError("error1"), "on_error_callback"),
        (NeptuneRetryableError("error1"), "on_warning_callback"),
        (ValueError("error2"), "on_error_callback"),
        (NeptuneScaleWarning("error3"), "on_warning_callback"),
        (NeptuneSeriesPointDuplicate("error4"), "on_warning_callback"),
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
    errors_handler = CustomErrorsHandler(**{callback_name: callback_with_event})
    errors_monitor = ErrorsMonitor(errors_queue=errors_queue, errors_handler=errors_handler)
    errors_monitor.start()

    # when
    errors_queue.put(error)

    # then
    assert callback_called.wait(timeout=5), "Callback was not called within the timeout"

    # and - cleanup
    errors_monitor.interrupt()
    errors_monitor.join(timeout=5)

    # then
    callback.assert_called()


@pytest.mark.parametrize(
    ["error", "callback_name"],
    [
        (NeptuneScaleError("error1"), "on_error_callback"),
        (NeptuneRetryableError("error1"), "on_warning_callback"),
        (ValueError("error2"), "on_error_callback"),
        (NeptuneScaleWarning("error3"), "on_warning_callback"),
        (NeptuneSeriesPointDuplicate("error4"), "on_warning_callback"),
        (NeptuneConnectionLostError("error6"), "on_network_error_callback"),
        (NeptuneAsyncLagThresholdExceeded("error7"), "on_async_lag_callback"),
        (NeptuneTooManyRequestsResponseError(), "on_warning_callback"),
    ],
)
def test_custom_errors_handler_callbacks_called(error, callback_name):
    # given
    callback = Mock()

    # and
    errors_handler = CustomErrorsHandler(**{callback_name: callback})

    # when
    errors_handler.handle(error)

    # then
    callback.assert_called()


@pytest.mark.parametrize(
    ["error", "action_name"],
    [
        (NeptuneScaleError("error1"), "on_error_action"),
        (NeptuneRetryableError("error1"), "on_warning_action"),
        (ValueError("error2"), "on_error_action"),
        (NeptuneScaleWarning("error3"), "on_warning_action"),
        (NeptuneSeriesPointDuplicate("error4"), "on_warning_action"),
        (NeptuneConnectionLostError("error6"), "on_network_error_action"),
        (NeptuneAsyncLagThresholdExceeded("error7"), "on_async_lag_action"),
        (NeptuneTooManyRequestsResponseError(), "on_warning_action"),
    ],
)
@pytest.mark.parametrize(
    "action_value",
    [RemoteErrorsHandlerAction.HANDLE, RemoteErrorsHandlerAction.SEND],
)
def test_remote_errors_handler_callbacks_called(error, action_name, action_value):
    # given
    errors_queue = Mock()

    # and
    errors_handler = RemoteErrorsHandler(**{"errors_queue": errors_queue, action_name: action_value})

    # when
    errors_handler.handle(error)

    # then
    if action_value == RemoteErrorsHandlerAction.SEND:
        errors_queue.put.assert_called()
    else:
        errors_queue.put.assert_not_called()
