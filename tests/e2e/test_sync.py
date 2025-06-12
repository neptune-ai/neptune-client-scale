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

from .conftest import (
    random_series,
    sleep_10s,
    unique_path,
)
from .test_fetcher import (
    fetch_attribute_values,
    fetch_files,
    fetch_metric_values,
    fetch_series_values,
)

TEST_TIMEOUT = 30


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


def test_sync_metrics(run_init_kwargs, client, project_name):
    # given
    run_id = run_init_kwargs["run_id"]
    with Run(**run_init_kwargs, mode="offline") as run:
        db_path = run._operations_repo._db_path

        metric_path = unique_path("test_sync_metrics/float_series")
        steps, values = random_series()
        for step, value in zip(steps, values):
            run.log_metrics(data={metric_path: value}, step=step)

    # when
    sync.sync_all(run_log_file=db_path, api_token=API_TOKEN)

    # then
    assert not db_path.exists()
    fetched = fetch_metric_values(client=client, project=project_name, custom_run_id=run_id, attributes=[metric_path])
    assert list(fetched[metric_path].keys()) == steps
    assert list(fetched[metric_path].values()) == values


def test_sync_series(run_init_kwargs, client, project_name):
    # given
    run_id = run_init_kwargs["run_id"]
    with Run(**run_init_kwargs, mode="offline") as run:
        db_path = run._operations_repo._db_path

        series_path = unique_path("test_sync_series/string_series")
        steps, values = random_series()
        values = [f"s{value}" for value in values]
        for step, value in zip(steps, values):
            run.log_string_series(data={series_path: value}, step=step)

    # when
    sync.sync_all(run_log_file=db_path, api_token=API_TOKEN)

    # then
    assert not db_path.exists()
    fetched = fetch_series_values(client=client, project=project_name, custom_run_id=run_id, attributes=[series_path])
    assert list(fetched[series_path].keys()) == steps
    assert list(fetched[series_path].values()) == values


def test_sync_files(run_init_kwargs, client, project_name, temp_dir):
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
    fetch_files(
        client=client, project=project_name, custom_run_id=run_id, attributes_targets={path: temp_dir / "file-value"}
    )
    with open(temp_dir / "file-value", "rb") as file:
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
@pytest.mark.timeout(TEST_TIMEOUT)
@patch("neptune_scale.cli.sync.run_sync_process", new=sleep_10s)  # replace the sync process with no-op sleep
def test_sync_wait_timeout(run_init_kwargs, timeout):
    # given
    with Run(**run_init_kwargs, mode="offline") as run:
        db_path = run._operations_repo._db_path
        run.log_configs(data={"str-value": "hello-world"})
        run.assign_files(files={"a-file": b"content"})

    runner = SyncRunner(api_token=API_TOKEN, run_log_file=db_path)
    runner.start()

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
        "neptune_scale.sync.errors_tracking.ErrorsMonitor.interrupt",
    ],
)
@pytest.mark.timeout(TEST_TIMEOUT)
def test_sync_stop_timeout(run_init_kwargs, timeout, hung_method):
    # given
    with Run(**run_init_kwargs, mode="offline") as run:
        db_path = run._operations_repo._db_path
        run.log_configs(data={"str-value": "hello-world"})
        run.assign_files(files={"a-file": b"content"})

    runner = SyncRunner(api_token=API_TOKEN, run_log_file=db_path)
    runner.start()

    # when
    with patch(hung_method):
        start_time = time.monotonic()
        runner.stop(timeout=timeout)
        elapsed_time = time.monotonic() - start_time

    # then
    assert timeout <= elapsed_time < timeout + 1
