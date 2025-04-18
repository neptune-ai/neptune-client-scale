import os
import time
from datetime import (
    datetime,
    timezone,
)
from unittest.mock import patch

import pytest

from neptune_scale.api.run import Run
from neptune_scale.cli import sync
from neptune_scale.cli.sync import SyncRunner
from neptune_scale.exceptions import NeptuneUnableToLogData
from neptune_scale.util import SharedInt

from .conftest import (
    random_series,
    unique_path,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
API_TOKEN = os.getenv("NEPTUNE_API_TOKEN")


def test_sync_empty_file(run_init_kwargs):
    with Run(**run_init_kwargs, mode="offline") as run:
        db_path = run._operations_repo._db_path

    sync.sync_all(run_log_file=db_path, api_token=API_TOKEN)


def test_sync_nonexistent_file(run_init_kwargs, tmp_path):
    path = tmp_path / "does-not-exist.db"

    with pytest.raises(FileNotFoundError):
        sync.sync_all(run_log_file=path, api_token=API_TOKEN)


def test_sync_invalid_file(tmp_path):
    path = tmp_path / "invalid.db"
    with open(path, "w") as f:
        f.write("invalid")

    with pytest.raises(NeptuneUnableToLogData):
        sync.sync_all(run_log_file=path, api_token=API_TOKEN)


def test_sync_atoms(run_init_kwargs, ro_run):
    # given
    with Run(**run_init_kwargs, mode="offline") as run:
        db_path = run._operations_repo._db_path
        now = time.time()
        data = {
            "int-value": int(now),
            "float-value": now,
            "str-value": f"hello-{now}",
            "true-value": True,
            "false-value": False,
            "datetime-value": datetime.now(timezone.utc).replace(microsecond=0),
        }
        run.log_configs(data)

    # when
    sync.sync_all(run_log_file=db_path, api_token=API_TOKEN)

    # then
    for key, value in data.items():
        assert ro_run[key].fetch() == value, f"Value for {key} does not match"


def test_sync_series(run_init_kwargs, ro_run):
    # given
    with Run(**run_init_kwargs, mode="offline") as run:
        db_path = run._operations_repo._db_path

        path = unique_path("test_series/series_no_prefetch")

        steps, values = random_series()
        for step, value in zip(steps, values):
            run.log_metrics(data={path: value}, step=step)

    # when
    sync.sync_all(run_log_file=db_path, api_token=API_TOKEN)

    # then
    df = ro_run[path].fetch_values()
    assert df["step"].tolist() == steps
    assert df["value"].tolist() == values


@pytest.mark.parametrize("timeout", [0, 1, 5])
@pytest.mark.timeout(30)
def test_sync_wait_timeout(run_init_kwargs, ro_run, timeout):
    # given
    with Run(**run_init_kwargs, mode="offline") as run:
        db_path = run._operations_repo._db_path
        data = {"str-value": "hello-world"}
        run.log_configs(data)
    runner = SyncRunner(api_token=API_TOKEN, run_log_file=db_path)
    runner.start()
    runner._last_ack_seq = SharedInt(1)  # replace so that wait never sees the true progress and hangs

    # when
    start_time = time.monotonic()
    runner.wait(timeout=timeout)
    elapsed_time = time.monotonic() - start_time

    # then
    assert timeout <= elapsed_time < timeout + 1


@pytest.mark.parametrize(
    "timeout",
    [
        0,
        1,
        5,
    ],
)
@pytest.mark.parametrize(
    "hung_method",
    [
        "neptune_scale.sync.sync_process.SyncProcess.terminate",
        "neptune_scale.sync.errors_tracking.ErrorsMonitor.interrupt",
    ],
)
@pytest.mark.timeout(30)
def test_sync_stop_timeout(run_init_kwargs, ro_run, timeout, hung_method):
    # given
    with Run(**run_init_kwargs, mode="offline") as run:
        db_path = run._operations_repo._db_path
        data = {"str-value": "hello-world"}
        run.log_configs(data)
    runner = SyncRunner(api_token=API_TOKEN, run_log_file=db_path)
    runner.start()

    # when
    with patch(hung_method):
        start_time = time.monotonic()
        runner.stop(timeout=timeout)
        elapsed_time = time.monotonic() - start_time

    # then
    assert timeout <= elapsed_time < timeout + 1
