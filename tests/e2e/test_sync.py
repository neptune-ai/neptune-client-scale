import os
import time
from datetime import (
    datetime,
    timezone,
)
from unittest.mock import patch

import pytest
from neptune_fetcher.alpha import (
    filters,
    runs,
)

from neptune_scale.api.run import Run
from neptune_scale.cli import sync
from neptune_scale.cli.sync import SyncRunner
from neptune_scale.exceptions import NeptuneUnableToLogData
from neptune_scale.util import SharedInt
from tests.e2e.test_fetcher.attribute_values import fetch_attribute_values

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


def test_sync_atoms(run_init_kwargs, client, project_name):
    # given
    run_id = run_init_kwargs["run_id"]
    with Run(**run_init_kwargs, mode="offline") as run:
        db_path = run._operations_repo._db_path
        now = time.time()
        data = {
            "test_sync_atoms/int-value": int(now),
            "test_sync_atoms/float-value": now,
            "test_sync_atoms/str-value": f"hello-{now}",
            "test_sync_atoms/true-value": True,
            "test_sync_atoms/false-value": False,
            "test_sync_atoms/datetime-value": datetime.now(timezone.utc).replace(microsecond=0),
        }
        run.log_configs(data)

    # when
    sync.sync_all(run_log_file=db_path, api_token=API_TOKEN)

    # then
    assert not db_path.exists()
    fetched = fetch_attribute_values(client=client, project=project_name, custom_run_id=run_id, attributes=data.keys())

    for key, value in data.items():
        assert fetched[key] == value, f"Value for {key} does not match"


def test_sync_series(run_init_kwargs, ro_run):
    # given
    with Run(**run_init_kwargs, mode="offline") as run:
        db_path = run._operations_repo._db_path

        series_path = unique_path("test_sync_series/test_sync_series")
        steps, values = random_series()
        for step, value in zip(steps, values):
            run.log_metrics(data={series_path: value}, step=step)

    # when
    sync.sync_all(run_log_file=db_path, api_token=API_TOKEN)

    # then
    assert not db_path.exists()
    df = ro_run[series_path].fetch_values()
    assert df["step"].tolist() == steps
    assert df["value"].tolist() == values


def test_sync_files(run_init_kwargs, temp_dir):
    # given
    run_id = run_init_kwargs["run_id"]
    with Run(**run_init_kwargs, mode="offline") as run:
        db_path = run._operations_repo._db_path

        path = "test_sync_files/file-value"
        run.assign_files(files={path: b"test_sync_files file content"})

    # when
    sync.sync_all(run_log_file=db_path, api_token=API_TOKEN)

    # then
    assert not db_path.exists()
    runs.download_files(runs=run_id, attributes=filters.AttributeFilter(name_eq=path), destination=temp_dir)
    expected_path = temp_dir / run_id / path.replace(":", "_").replace("+", "_")
    with open(expected_path, "rb") as file:
        content = file.read()
        assert content == b"test_sync_files file content"


def test_sync_all_types_combined(run_init_kwargs, temp_dir):
    # given
    with Run(**run_init_kwargs, mode="offline") as run:
        db_path = run._operations_repo._db_path

        now = time.time()
        data = {
            "test_sync_all_types/int-value": int(now),
            "test_sync_all_types/float-value": now,
            "test_sync_all_types/str-value": f"hello-{now}",
            "test_sync_all_types/true-value": True,
            "test_sync_all_types/false-value": False,
            "test_sync_all_types/datetime-value": datetime.now(timezone.utc).replace(microsecond=0),
        }
        run.log_configs(data)

        series_path = unique_path("test_sync_all_types/test_sync_all_types")
        steps, values = random_series()
        for step, value in zip(steps, values):
            run.log_metrics(data={series_path: value}, step=step)

        run.assign_files(files={"test_sync_all_types/file-value": b"test_sync_all_types file content"})

    # when
    sync.sync_all(run_log_file=db_path, api_token=API_TOKEN)

    # then
    assert not db_path.exists()


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
