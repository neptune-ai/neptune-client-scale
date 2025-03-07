import time
import uuid
from unittest.mock import Mock

import pytest
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import Value

from neptune_scale import Run
from neptune_scale.exceptions import NeptuneSynchronizationInterrupted
from neptune_scale.sync.operations_repository import OperationsRepository
from neptune_scale.sync.parameters import MAX_SINGLE_OPERATION_SIZE_BYTES
from neptune_scale.util import envs

# Assume that this is how long we allow for a run to be created
# plus the time for the rest of the test.
# The tests follow the same pattern:
#    create run -> kill sync process -> use functions that should  not block
#
# The timeout will detect if we block on run.wait*() or run.close().
#
# In case of backend problems we _will_ get false positives here, but for simplicity
# this is how we approach this. Once we remove the .wait_for_processing() call
# from Run.__init__, the backend will not be a factor anymore.
TEST_TIMEOUT = 30

run_id = "de3c6049-9243-43bf-be37-d4771099ac15"


def _kill_sync_process(run, *, after=None):
    run._sync_process.kill()
    run._sync_process.join(timeout=5)

    if run._sync_process.exitcode is None:
        raise RuntimeError("SyncProcess did not terminate")


@pytest.fixture(autouse=True)
def use_temp_db_dir(temp_dir, monkeypatch):
    monkeypatch.setenv(envs.LOG_DIRECTORY, str(temp_dir))
    yield


def test_warning_callback_after_sync_process_dies(caplog):
    callback = Mock()
    run = Run(run_id=str(uuid.uuid4()), on_warning_callback=callback)
    _kill_sync_process(run)

    # We need to delay here a bit. Even though process is not dead, the monitoring threads
    # could still be processing that information, and we have not good means to wait for that.
    time.sleep(0.5)

    callback.assert_called_once()
    assert isinstance(callback.call_args[0][0], NeptuneSynchronizationInterrupted)


@pytest.mark.timeout(TEST_TIMEOUT)
def test_run_can_log_after_sync_process_dies():
    run = Run(run_id=str(uuid.uuid4()))
    run.log_metrics({"metric": 2}, step=1)
    run.wait_for_processing()

    _kill_sync_process(run)

    run.log_metrics({"metric": 4}, step=2)
    run.log_metrics({"metric": 6}, step=3)
    run.close()

    repo = OperationsRepository(run._operations_repo._db_path)
    operations = repo.get_operations(MAX_SINGLE_OPERATION_SIZE_BYTES)

    assert len(operations) == 2
    assert operations[0].operation.append == {"metric": Value(float64=4.0)}
    assert operations[1].operation.append == {"metric": Value(float64=6.0)}


@pytest.mark.timeout(TEST_TIMEOUT)
def test_run_wait_methods_after_sync_process_dies():
    run = Run(run_id=str(uuid.uuid4()))
    repo = OperationsRepository(run._operations_repo._db_path)

    _kill_sync_process(run)

    run.log_metrics({"metric": 2}, step=1)
    run.wait_for_submission()
    assert len(repo.get_operations(MAX_SINGLE_OPERATION_SIZE_BYTES)) == 1

    run.log_metrics({"metric": 4}, step=2)
    run.wait_for_processing()
    assert len(repo.get_operations(MAX_SINGLE_OPERATION_SIZE_BYTES)) == 2

    run.log_metrics({"metric": 6}, step=3)
    run.close()

    operations = repo.get_operations(MAX_SINGLE_OPERATION_SIZE_BYTES)
    assert len(operations) == 3
    for i in range(1, 4):
        assert operations[i - 1].operation.append == {"metric": Value(float64=i * 2.0)}


@pytest.mark.timeout(TEST_TIMEOUT)
def test_run_terminate_after_sync_process_dies():
    run = Run(run_id=str(uuid.uuid4()))
    _kill_sync_process(run)

    # Should return quickly, otherwise the timeout will trigger
    run.terminate()
