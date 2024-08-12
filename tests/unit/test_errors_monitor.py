from unittest.mock import Mock

from neptune_scale.core.components.errors_monitor import ErrorsMonitor
from neptune_scale.core.components.errors_queue import ErrorsQueue


def test_errors_monitor():
    # given
    callback = Mock()

    # and
    errors_queue = ErrorsQueue()
    errors_monitor = ErrorsMonitor(errors_queue=errors_queue, on_error_callback=callback)

    # when
    errors_queue.put(ValueError("error1"))
    errors_monitor.start()
    errors_monitor.join(timeout=1)

    # then
    callback.assert_called()
