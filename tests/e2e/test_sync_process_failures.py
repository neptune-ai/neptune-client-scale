import multiprocessing
import os
import threading
import time
import uuid
from unittest.mock import (
    Mock,
    patch,
)

import pytest

from neptune_scale import Run
from neptune_scale.exceptions import NeptuneSynchronizationStopped
from neptune_scale.sync.operations_repository import (
    OperationsRepository,
    OperationType,
)
from neptune_scale.sync.parameters import MAX_SINGLE_OPERATION_SIZE_BYTES
from neptune_scale.util import (
    ProcessLink,
    SharedInt,
    envs,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")

# Timeout value for all the tests.
#
# The tests follow the following pattern:
#    create run -> kill sync process -> use functions that should not block
#
# The timeout will allow us to detect if we block on any Run methods after
# the child process is killed.
TEST_TIMEOUT = 20


def _kill_sync_process(run, after=None):
    if after is not None:
        time.sleep(after)

    run._sync_process.kill()
    run._sync_process.join(timeout=5)

    assert run._sync_process.exitcode is not None, "SyncProcess did not terminate"

    # We need to delay here a bit. Even though process is now dead, the monitoring threads
    # could still be processing that information buffered in queues, and we don't have
    # other better means to wait for that.
    time.sleep(0.5)


@pytest.fixture(autouse=True)
def use_temp_db_dir(temp_dir, monkeypatch):
    monkeypatch.setenv(envs.LOG_DIRECTORY, str(temp_dir))
    yield


def test_warning_callback_after_sync_process_dies():
    error_callback = Mock()
    run = Run(run_id=str(uuid.uuid4()), on_error_callback=error_callback)
    _kill_sync_process(run)

    error_callback.assert_called_once()
    assert isinstance(error_callback.call_args.args[0], NeptuneSynchronizationStopped)


@pytest.mark.timeout(TEST_TIMEOUT)
def test_run_can_log_after_sync_process_dies():
    run = Run(run_id=str(uuid.uuid4()))
    run.log_metrics({"metric": 2}, step=1)
    run.wait_for_processing()

    _kill_sync_process(run)

    run.log_metrics({"metric": 4}, step=2)
    run.log_metrics({"metric": 6}, step=3)
    run.log_configs({"config": "foo"})
    run.add_tags(["tag1", "tag2"])
    run.close()

    repo = OperationsRepository(run._operations_repo._db_path)
    operations = repo.get_operations(MAX_SINGLE_OPERATION_SIZE_BYTES)

    assert len(operations) == 4
    assert operations[0].operation.append["metric"].float64 == 4.0
    assert operations[1].operation.append["metric"].float64 == 6.0
    assert operations[2].operation.assign["config"].string == "foo"
    assert sorted(operations[3].operation.modify_sets["sys/tags"].string.values.keys()) == ["tag1", "tag2"]


@pytest.mark.timeout(TEST_TIMEOUT)
@pytest.mark.parametrize("wait_for_submission", (True, False))
@pytest.mark.parametrize("wait_for_processing", (True, False))
def test_run_wait_methods_after_sync_process_dies(wait_for_submission, wait_for_processing):
    run = Run(run_id=str(uuid.uuid4()))
    run.wait_for_processing()

    _kill_sync_process(run)

    run.log_metrics({"metric": 2}, step=1)
    if wait_for_submission:
        run.wait_for_submission()

    run.log_metrics({"metric": 4}, step=2)
    if wait_for_processing:
        run.wait_for_processing()

    run.log_metrics({"metric": 6}, step=3)
    run.close()

    repo = OperationsRepository(run._operations_repo._db_path)
    operations = repo.get_operations(MAX_SINGLE_OPERATION_SIZE_BYTES)

    assert len(operations) == 3
    assert operations[0].operation.append["metric"].float64 == 2.0
    assert operations[1].operation.append["metric"].float64 == 4.0
    assert operations[2].operation.append["metric"].float64 == 6.0


@pytest.mark.timeout(TEST_TIMEOUT)
def test_sync_process_dies_after_sync_thread_dies():
    run = Run(run_id=str(uuid.uuid4()), api_token="fake")

    # fake token should cause SyncThread to die

    run.wait_for_processing()  # assert that it ends


class MockSyncProcess(multiprocessing.Process):
    """A SyncProcess mock that does nothing except:

    * starting the process link and staying alive until killed
    * confirming only the 1st operation, which is CreateRun submitted
      and waited for in Run.__init__()

    We need it in test_run_wait_methods_after_sync_process_dies_during_wait(),
    to achieve the "never send or confirm any operations" behaviour. Other tests
    use the standard SyncProcess to minimize interference with regular operations.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(name="MockSyncProcess")

        self._process_link: ProcessLink = kwargs["process_link"]
        self._last_ack_seq: SharedInt = kwargs["last_ack_seq"]

    def run(self):
        self._process_link.start()

        # Confirm the first operation, which is Run creation, so Run.__init__() can complete
        self._last_ack_seq.value = 1
        self._last_ack_seq.notify_all()

        while True:
            time.sleep(1)


@pytest.mark.timeout(TEST_TIMEOUT)
@pytest.mark.parametrize("wait_for_submission", (True, False))
@pytest.mark.parametrize("wait_for_processing", (True, False))
@patch("neptune_scale.api.run.SyncProcess", new=MockSyncProcess)
def test_run_wait_methods_after_sync_process_dies_during_wait(wait_for_submission, wait_for_processing):
    """Kill the child process during wait(), to make sure we're not blocked forever in this scenario."""

    run = Run(run_id=str(uuid.uuid4()))
    thread = threading.Thread(target=_kill_sync_process, args=(run, 3.0))
    thread.start()

    run.log_metrics({"metric": 2}, step=1)
    if wait_for_submission:
        run.wait_for_submission()

    if wait_for_processing:
        run.wait_for_processing()

    thread.join()

    # At this point the process should be dead
    assert not run._sync_process.is_alive()

    run.log_metrics({"metric": 4}, step=2)
    run.close()

    repo = OperationsRepository(run._operations_repo._db_path)
    operations = repo.get_operations(MAX_SINGLE_OPERATION_SIZE_BYTES)

    assert len(operations) == 3
    assert operations[0].operation_type == OperationType.CREATE_RUN
    assert operations[1].operation.append["metric"].float64 == 2.0
    assert operations[2].operation.append["metric"].float64 == 4.0


@pytest.mark.timeout(TEST_TIMEOUT)
def test_run_terminate_after_sync_process_dies():
    run = Run(run_id=str(uuid.uuid4()))
    _kill_sync_process(run)

    # Should return quickly, otherwise the timeout will trigger
    run.terminate()
