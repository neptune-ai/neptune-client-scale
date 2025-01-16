import threading
from unittest.mock import patch

import pytest

from neptune_scale import Run
from neptune_scale.exceptions import NeptuneUnexpectedError
from neptune_scale.sync.errors_tracking import ErrorsQueue


@pytest.fixture
def run(api_token):
    return Run(project="workspace/project", api_token=api_token, run_id="test", mode="disabled")


@pytest.mark.timeout(10)
def test_multiple_closes_single_thread(run):
    """This should not block, hence the timeout check"""

    run.close()
    run.close()


@pytest.mark.timeout(10)
def test_multiple_closes_multiple_threads(run):
    """Close in one thread should block close in another thread"""

    closed = threading.Event()

    def closing_thread():
        # Should block until the first close is done, and return False, as not all operations are done
        assert not run.close(), "Run.close() returned True"
        assert closed.wait(timeout=1), "wait_for_processing() finished before close()"

    th = threading.Thread(target=closing_thread, daemon=True)

    run.close()
    th.start()
    closed.set()

    th.join(timeout=1)

    assert not th.is_alive(), "Run.wait_for_processing() did not return in time after close()"


@pytest.mark.timeout(10)
def test_wait_for_processing_aborts_if_closed(run):
    closed = threading.Event()

    def waiting_thread():
        assert not run.wait_for_processing(timeout=5)
        assert closed.wait(timeout=1), "wait_for_processing() finished before close()"

    th = threading.Thread(target=waiting_thread, daemon=True)

    run.close()
    th.start()
    closed.set()

    th.join(timeout=1)

    assert not th.is_alive(), "Run.wait_for_processing() did not return in time after close()"


@pytest.mark.timeout(10)
def test_terminate_on_error(api_token):
    """When calling Run.terminate() from the error callback, the run should terminate properly
    without deadlocking"""

    callback_called = threading.Event()
    callback_finished = threading.Event()

    def callback(exc, ts):
        assert isinstance(exc, NeptuneUnexpectedError)
        assert "Expected error" in str(exc)

        callback_called.set()
        run.terminate()
        callback_finished.set()

    run = Run(
        project="workspace/project", api_token=api_token, run_id="test", mode="disabled", on_error_callback=callback
    )

    # Pretend we've sent an operation
    run._last_queued_seq.value += 1
    run._errors_queue.put(ValueError("Expected error"))

    assert callback_called.wait(timeout=1)
    run.wait_for_processing(timeout=1)
    assert callback_finished.wait(timeout=10)


@pytest.mark.timeout(10)
def test_run_creation_during_initialization_error(api_token):
    """If there's an error when creating a Run (with resume=False), the error callback should be called,
    and it should be safe to terminate the Run
    """
    callback_finished = threading.Event()

    def callback(exc, ts):
        run.terminate()
        callback_finished.set()

    errors_queue = ErrorsQueue()

    def _create_run(*args, **kwargs):
        # This method is called by Run.__init__ to create a run. Instead of submitting a
        # CreateRun operation, we simulate an error
        errors_queue.put(ValueError("Expected error"))

    with (
        patch("neptune_scale.api.run.ErrorsQueue", return_value=errors_queue),
        patch.object(Run, "_create_run", side_effect=_create_run),
    ):
        run = Run(
            project="workspace/project", api_token=api_token, run_id="test", mode="disabled", on_error_callback=callback
        )

    assert callback_finished.wait(timeout=10)
    # The run should be terminated, so wait_for_processing should return False
    assert not run.wait_for_processing(timeout=1)
