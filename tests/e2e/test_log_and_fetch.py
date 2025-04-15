import math
import os
import threading
import time
from datetime import (
    datetime,
    timezone,
)

import numpy as np
import pytest
from neptune_fetcher import ReadOnlyRun
from neptune_fetcher.alpha import runs
from pytest import mark

from neptune_scale.api.run import Run

from .conftest import (
    random_series,
    unique_path,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
SYNC_TIMEOUT = 30


def refresh(ro_run: ReadOnlyRun):
    """Create a new ReadOnlyRun instance with the same project and custom_id,
    which is basically a "refresh" operation"""
    return ReadOnlyRun(read_only_project=ro_run.project, custom_id=ro_run["sys/custom_run_id"].fetch())


def test_atoms(run, ro_run):
    """Set atoms to a value, make sure it's equal when fetched"""

    now = time.time()
    data = {
        "int-value": int(now),
        "float-value": now,
        "str-value": f"hello-{now}",
        "true-value": True,
        "false-value": False,
        # The backend rounds the milliseconds component, so we're fine with just 0 to be more predictable
        "datetime-value": datetime.now(timezone.utc).replace(microsecond=0),
    }

    run.log_configs(data)
    run.wait_for_processing(SYNC_TIMEOUT)

    for key, value in data.items():
        assert ro_run[key].fetch() == value, f"Value for {key} does not match"

    # Replace the data and make sure the update is reflected AFTER we purge the cache for those items
    updated_data = {
        "int-value": int(now + 1),
        "float-value": now + 1,
        "str-value": f"hello-{now + 1}",
        "true-value": False,
        "false-value": True,
        "datetime-value": datetime.now(timezone.utc).replace(year=1999, microsecond=0),
    }

    run.log_configs(updated_data)
    run.wait_for_processing(SYNC_TIMEOUT)

    # The data should stay the same, as we haven't purged the cache yet
    for key, value in data.items():
        assert ro_run[key].fetch() == value, f"The cached value for {key} does not match"

    # Now purge the cache for the logged items, and fetch them again. They should now have the new values
    for key in data.keys():
        del ro_run[key]

    for key, value in updated_data.items():
        assert ro_run[key].fetch() == value, f"The updated value for {key} does not match"


def test_series_no_prefetch(run, ro_run):
    path = unique_path("test_series/series_no_prefetch")

    steps, values = random_series()

    for step, value in zip(steps, values):
        run.log_metrics(data={path: value}, step=step)

    run.wait_for_processing(SYNC_TIMEOUT)

    df = ro_run[path].fetch_values()
    assert df["step"].tolist() == steps
    assert df["value"].tolist() == values


def test_single_series_with_prefetch(run, ro_run):
    path = unique_path("test_series/series_with_prefetch")

    steps, values = random_series()

    for step, value in zip(steps, values):
        run.log_metrics(data={path: value}, step=step)

    run.wait_for_processing(SYNC_TIMEOUT)

    ro_run.prefetch_series_values([path], use_threads=True)
    df = ro_run[path].fetch_values()

    assert df["step"].tolist() == steps
    assert df["value"].tolist() == values


def test_multiple_series_with_prefetch(run, ro_run):
    path_base = unique_path("test_series/many_series_with_prefetch")
    data = {f"{path_base}-{i}": i for i in range(20)}

    run.log_metrics(data, step=1)
    run.wait_for_processing(SYNC_TIMEOUT)

    ro_run = refresh(ro_run)
    paths = [p for p in ro_run.field_names if p.startswith(path_base)]
    assert len(paths) == len(data), "Not all data was logged"

    ro_run.prefetch_series_values(paths, use_threads=True)
    for path in paths:
        df = ro_run[path].fetch_values()
        assert df["step"].tolist() == [1]
        assert df["value"].tolist() == [data[path]]


def test_series_fetch_and_append(run, ro_run):
    """Fetch a series, then append, then fetch again -- the new data points should be there"""

    path = unique_path("test_series/series_no_prefetch")

    steps, values = random_series()

    for step, value in zip(steps, values):
        run.log_metrics(data={path: value}, step=step)

    run.wait_for_processing(SYNC_TIMEOUT)

    df = ro_run[path].fetch_values()
    assert df["step"].tolist() == steps
    assert df["value"].tolist() == values

    steps2, values2 = random_series(length=5, start_step=len(steps))

    for step, value in zip(steps2, values2):
        run.log_metrics(data={path: value}, step=step)

    run.wait_for_processing(SYNC_TIMEOUT)

    df = ro_run[path].fetch_values()
    assert df["step"].tolist() == steps + steps2
    assert df["value"].tolist() == values + values2


@mark.parametrize("value", [np.inf, -np.inf, np.nan, math.inf, -math.inf, math.nan])
def test_single_non_finite_metric(value, run, ro_run):
    path = unique_path("test_series/non_finite")

    run.log_metrics(data={path: value}, step=1)
    run.wait_for_processing(SYNC_TIMEOUT)
    assert path not in refresh(ro_run).field_names


def test_single_file(run, run_init_kwargs, temp_dir):
    # given
    files = {"test_files/file": b"Hello world"}

    # when
    run.assign_files(files)
    run.wait_for_processing(SYNC_TIMEOUT)

    # then
    assert run._operations_repo.get_file_upload_requests_count() == 0
    runs.download_files(runs=run_init_kwargs["run_id"], attributes="test_files/file", destination=temp_dir)
    # todo: check the file content


def test_async_lag_callback():
    event = threading.Event()
    with Run(
        project=NEPTUNE_PROJECT,
        async_lag_threshold=0.000001,
        on_async_lag_callback=lambda: event.set(),
    ) as run:
        run.wait_for_processing(SYNC_TIMEOUT)

        # First callback should be called after run creation
        event.wait(timeout=60)
        assert event.is_set()
        event.clear()

        run.log_configs(
            data={
                "parameters/learning_rate": 0.001,
                "parameters/batch_size": 64,
            },
        )
        # Second callback should be called after logging configs
        event.wait(timeout=60)
        assert event.is_set()
