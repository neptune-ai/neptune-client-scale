import os
import time
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
from neptune_scale.util import envs
from tests.e2e.conftest import sleep_3s

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")

# Timeout value for all the tests.
#
# The tests follow the following pattern:
#    create run -> kill sync process -> use functions that should not block
#
# The timeout will allow us to detect if we block on any Run methods after
# the child process is killed.
TEST_TIMEOUT = 30


@pytest.fixture(autouse=True)
def use_temp_db_dir(temp_dir, monkeypatch):
    monkeypatch.setenv(envs.LOG_DIRECTORY, str(temp_dir))
    yield


@pytest.mark.timeout(TEST_TIMEOUT)
@patch("neptune_scale.api.run.run_sync_process", new=sleep_3s)  # replace the sync process with no-op sleep
def test_warning_callback_after_sync_process_dies():
    error_callback = Mock()
    run = Run(on_error_callback=error_callback)
    run._sync_process.join()

    time.sleep(1)  # give some time for the error callback to be called

    error_callback.assert_called_once()
    assert isinstance(error_callback.call_args.args[0], NeptuneSynchronizationStopped)


@pytest.mark.timeout(TEST_TIMEOUT)
@pytest.mark.parametrize("wait_for_submission", (True, False))
@pytest.mark.parametrize("wait_for_processing", (True, False))
@pytest.mark.parametrize("wait_for_file_upload", (True, False))
@patch("neptune_scale.api.run.run_sync_process", new=sleep_3s)  # replace the sync process with no-op sleep
def test_run_wait_methods_after_sync_process_dies(wait_for_submission, wait_for_processing, wait_for_file_upload):
    run = Run()
    run._sync_process.join()
    assert not run._sync_process.is_alive()

    run.log_metrics({"metric": 2}, step=1)
    if wait_for_submission:
        assert not run.wait_for_submission()

    run.log_metrics({"metric": 4}, step=2)
    if wait_for_processing:
        assert not run.wait_for_processing()

    run.assign_files(files={"a-file": b"content"})
    if wait_for_file_upload:
        assert not run._wait_for_file_upload()

    run.close()


@pytest.mark.timeout(TEST_TIMEOUT)
def test_sync_process_dies_after_sync_thread_dies():
    run = Run(api_token="fake")

    # fake token should cause SyncThread to die

    assert not run.wait_for_processing()  # assert that it ends


@pytest.mark.timeout(TEST_TIMEOUT)
@pytest.mark.parametrize("wait_for_submission", (True, False))
@pytest.mark.parametrize("wait_for_processing", (True, False))
@pytest.mark.parametrize("wait_for_file_upload", (True, False))
@patch("neptune_scale.api.run.run_sync_process", new=sleep_3s)  # replace the sync process with no-op sleep
def test_run_wait_methods_after_sync_process_dies_during_wait(
    wait_for_submission, wait_for_processing, wait_for_file_upload
):
    """Make sure we're not blocked forever if the sync process dies before completing all the work."""

    run = Run()
    run.log_metrics({"metric": 2}, step=1)
    run.assign_files(files={"a-file": b"content"})

    if wait_for_submission:
        assert not run.wait_for_submission()

    if wait_for_processing:
        assert not run.wait_for_processing()

    if wait_for_file_upload:
        assert not run._wait_for_file_upload()

    # only assert the process is dead if we did actually wait
    if wait_for_processing or wait_for_submission or wait_for_file_upload:
        assert not run._sync_process.is_alive()

    run.close()


@pytest.mark.timeout(TEST_TIMEOUT)
@patch("neptune_scale.api.run.run_sync_process", new=sleep_3s)  # replace the sync process with no-op sleep
def test_run_writable_after_sync_process_dies():
    run = Run()
    run._sync_process.join()

    run.log_metrics({"metric": 2}, step=2)
    run.log_configs({"config": "foo"})
    run.add_tags(["tag1", "tag2"])
    run.assign_files(files={"a-file": b"content"})

    repo = OperationsRepository(run._operations_repo._db_path)
    operations = repo.get_operations(MAX_SINGLE_OPERATION_SIZE_BYTES)

    assert len(operations) == 6
    assert operations[0].operation_type == OperationType.CREATE_RUN
    assert operations[1].operation.assign["source_code/branch"].string
    assert operations[2].operation.append["metric"].float64 == 2.0
    assert operations[3].operation.assign["config"].string == "foo"
    assert sorted(operations[4].operation.modify_sets["sys/tags"].string.values.keys()) == ["tag1", "tag2"]
    assert operations[5].operation.assign["a-file"].file_ref.size_bytes == len(b"content")


@pytest.mark.timeout(TEST_TIMEOUT)
@patch("neptune_scale.api.run.run_sync_process", new=sleep_3s)  # replace the sync process with no-op sleep
def test_run_terminate_after_sync_process_dies():
    run = Run()
    run._sync_process.join()

    # Should return quickly, otherwise the timeout will trigger
    run.terminate()
