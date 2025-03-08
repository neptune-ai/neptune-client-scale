import os
import sqlite3
import time
from datetime import (
    datetime,
    timezone,
)

import pytest

from neptune_scale.api.run import Run
from neptune_scale.cli import sync

from .conftest import (
    random_series,
    unique_path,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
API_TOKEN = os.getenv("NEPTUNE_API_TOKEN")
SYNC_TIMEOUT = 30


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

    with pytest.raises(sqlite3.DatabaseError):
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
