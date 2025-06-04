import multiprocessing
import pathlib
import tempfile
import time
from threading import Event
from unittest.mock import Mock

import pytest
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
    UpdateRunSnapshot,
    Value,
)

from neptune_scale.sync.lag_tracking import LagTracker
from neptune_scale.sync.operations_repository import OperationsRepository

mp_context = multiprocessing.get_context("spawn")


@pytest.fixture
def temp_db_path():
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = pathlib.Path(temp_dir) / "test_operations.db"
        yield db_path


@pytest.fixture
def operations_repo(temp_db_path):
    repo = OperationsRepository(db_path=temp_db_path)
    repo.init_db()
    yield repo
    repo.close(cleanup_files=True)


def test__lag_tracker__callback_called(operations_repo):
    # given
    async_lag_threshold = 1.0

    # and
    snapshot = UpdateRunSnapshot(assign={"key": Value(string="value")})
    operations_repo.save_update_run_snapshots([snapshot])
    callback = Mock()

    # Synchronization event
    callback_called = Event()

    # Modify the callback to set the event when called
    def callback_with_event() -> None:
        callback()
        callback_called.set()

    # and
    lag_tracker = LagTracker(
        operations_repository=operations_repo,
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


def test__lag_tracker__not_called(operations_repo):
    # given
    async_lag_threshold = 10.0

    # and
    snapshot = UpdateRunSnapshot(assign={"key": Value(string="value")})
    operations_repo.save_update_run_snapshots([snapshot])
    callback = Mock()

    # and
    lag_tracker = LagTracker(
        operations_repository=operations_repo,
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
