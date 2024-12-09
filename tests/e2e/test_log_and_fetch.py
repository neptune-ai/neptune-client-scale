import math
import os
import random
import time
import uuid
from datetime import (
    datetime,
    timezone,
)

import numpy as np
from neptune_fetcher import ReadOnlyRun
from pytest import mark

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")


def unique_path(prefix):
    return f"{prefix}__{datetime.now(timezone.utc).isoformat('-', 'seconds')}__{str(uuid.uuid4())[-4:]}"


def refresh(ro_run: ReadOnlyRun):
    """Create a new ReadOnlyRun instance with the same project and custom_id,
    which is basically a "refresh" operation"""
    return ReadOnlyRun(read_only_project=ro_run.project, custom_id=ro_run["sys/custom_run_id"].fetch())


def random_series(length=10, start_step=0):
    """Return a 2-tuple of step and value lists, both of length `length`"""
    assert length > 0
    assert start_step >= 0

    j = random.random()
    # Round to 0 to avoid floating point errors
    steps = [round((j + x) ** 2.0, 0) for x in range(start_step, length)]
    values = [round((j + x) ** 3.0, 0) for x in range(len(steps))]

    return steps, values


def test_atoms(sync_run, ro_run):
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

    sync_run.log_configs(data)

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

    sync_run.log_configs(updated_data)

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

    run.wait_for_processing()

    df = ro_run[path].fetch_values()
    assert df["step"].tolist() == steps
    assert df["value"].tolist() == values


def test_single_series_with_prefetch(run, ro_run):
    path = unique_path("test_series/series_with_prefetch")

    steps, values = random_series()

    for step, value in zip(steps, values):
        run.log_metrics(data={path: value}, step=step)

    run.wait_for_processing()

    ro_run.prefetch_series_values([path], use_threads=True)
    df = ro_run[path].fetch_values()

    assert df["step"].tolist() == steps
    assert df["value"].tolist() == values


def test_multiple_series_with_prefetch(run, ro_run):
    path_base = unique_path("test_series/many_series_with_prefetch")
    data = {f"{path_base}-{i}": i for i in range(20)}

    run.log_metrics(data, step=1)
    run.wait_for_processing()

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

    run.wait_for_processing()

    df = ro_run[path].fetch_values()
    assert df["step"].tolist() == steps
    assert df["value"].tolist() == values

    steps2, values2 = random_series(length=5, start_step=len(steps))

    for step, value in zip(steps2, values2):
        run.log_metrics(data={path: value}, step=step)

    run.wait_for_processing()

    df = ro_run[path].fetch_values()
    assert df["step"].tolist() == steps + steps2
    assert df["value"].tolist() == values + values2


@mark.parametrize("value", [np.inf, -np.inf, np.nan, math.inf, -math.inf, math.nan])
def test_single_non_finite_metric(value, sync_run, ro_run):
    path = unique_path("test_series/non_finite")
    sync_run.log_metrics(data={path: value}, step=1)
    assert path not in refresh(ro_run).field_names


@mark.parametrize("first_step", [None, 0, 10])
def test_auto_step_with_initial_step(run, ro_run, first_step):
    """Logging series values with step=None results in backend-side step assignment"""

    path = unique_path(f"test_series/auto_step_{first_step}")

    _, values = random_series()

    run.log_metrics(data={path: values[0]}, step=first_step)
    for value in values[1:]:
        run.log_metrics(data={path: value})

    run.wait_for_processing()

    # Backend will assign steps starting from zero by default,
    # so handle this test case properly
    if first_step is None:
        first_step = 0

    df = ro_run[path].fetch_values()
    assert df["step"].tolist() == [float(x) for x in list(range(first_step, first_step + len(values)))]
    assert df["value"].tolist() == values


def test_auto_step_with_manual_increase(run, ro_run):
    """Increase step manually at a single point in series, then use auto-step"""

    path = unique_path("test_series/auto_step_increase")
    run.log_metrics(data={path: 1})
    run.log_metrics(data={path: 2}, step=10)
    run.log_metrics(data={path: 3})

    run.wait_for_processing()

    df = ro_run[path].fetch_values()
    assert df["step"].tolist() == [0, 10, 11]
    assert df["value"].tolist() == [1, 2, 3]


def test_auto_step_with_different_metrics(run, ro_run):
    path1 = unique_path("test_series/auto_step_different_metrics1")
    path2 = unique_path("test_series/auto_step_different_metrics2")

    run.log_metrics(data={path1: 1})
    run.log_metrics(data={path2: 1}, step=10)

    run.log_metrics(data={path1: 2})
    run.log_metrics(data={path2: 2}, step=20)

    run.log_metrics(data={path1: 3}, step=5)
    run.log_metrics(data={path2: 3})

    run.wait_for_processing()

    df1 = ro_run[path1].fetch_values()
    assert df1["step"].tolist() == [0.0, 1.0, 5.0]
    assert df1["value"].tolist() == [1, 2, 3]

    df2 = ro_run[path2].fetch_values()
    assert df2["step"].tolist() == [10.0, 20.0, 21.0]
    assert df2["value"].tolist() == [1, 2, 3]
