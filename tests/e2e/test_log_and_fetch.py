import math
import os
import threading
import time
from datetime import (
    datetime,
    timezone,
)

import numpy as np
from pytest import mark

from neptune_scale.api.run import Run
from neptune_scale.types import Histogram

from .conftest import (
    random_series,
    unique_path,
)
from .test_fetcher import (
    fetch_attribute_values,
    fetch_metric_values,
    fetch_series_values,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
SYNC_TIMEOUT = 30


def test_atoms(run, client, project_name):
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

    fetched = fetch_attribute_values(client, project_name, custom_run_id=run._run_id, attributes=data.keys())
    for key, value in data.items():
        assert fetched[key] == value, f"Value for {key} does not match"

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

    fetched = fetch_attribute_values(client, project_name, custom_run_id=run._run_id, attributes=data.keys())
    for key, value in updated_data.items():
        assert fetched[key] == value, f"The updated value for {key} does not match"


def test_metric(run, client, project_name):
    path = unique_path("test_metric/metric")

    steps, values = random_series()

    for step, value in zip(steps, values):
        run.log_metrics(data={path: value}, step=step)

    run.wait_for_processing(SYNC_TIMEOUT)

    fetched = fetch_metric_values(client=client, project=project_name, custom_run_id=run._run_id, attributes=[path])
    assert list(fetched[path].keys()) == steps
    assert list(fetched[path].values()) == values


def test_multiple_metrics(run, client, project_name):
    path_base = unique_path("test_metric/many_metrics")
    data = {f"{path_base}-{i}": i for i in range(20)}

    run.log_metrics(data, step=1)
    run.wait_for_processing(SYNC_TIMEOUT)

    fetched = fetch_metric_values(
        client=client, project=project_name, custom_run_id=run._run_id, attributes=data.keys()
    )
    assert len(fetched) == len(data), "Not all data was logged"

    for path, values in fetched.items():
        assert list(fetched[path].keys()) == [1]
        assert list(fetched[path].values()) == [data[path]]


def test_metric_fetch_and_append(run, client, project_name):
    """Fetch a series, then append, then fetch again -- the new data points should be there"""

    path = unique_path("test_series/series_no_prefetch")

    steps, values = random_series()

    for step, value in zip(steps, values):
        run.log_metrics(data={path: value}, step=step)

    run.wait_for_processing(SYNC_TIMEOUT)

    fetched = fetch_metric_values(client=client, project=project_name, custom_run_id=run._run_id, attributes=[path])
    assert list(fetched[path].keys()) == steps
    assert list(fetched[path].values()) == values

    steps2, values2 = random_series(length=5, start_step=len(steps))

    for step, value in zip(steps2, values2):
        run.log_metrics(data={path: value}, step=step)

    run.wait_for_processing(SYNC_TIMEOUT)

    fetched = fetch_metric_values(client=client, project=project_name, custom_run_id=run._run_id, attributes=[path])
    assert list(fetched[path].keys()) == steps + steps2
    assert list(fetched[path].values()) == values + values2


@mark.parametrize("value", [np.inf, -np.inf, np.nan, math.inf, -math.inf, math.nan])
def test_single_non_finite_metric(run, client, project_name, value):
    path = unique_path("test_series/non_finite")

    run.log_metrics(data={path: value}, step=1)
    run.wait_for_processing(SYNC_TIMEOUT)

    fetched = fetch_metric_values(client=client, project=project_name, custom_run_id=run._run_id, attributes=[path])
    assert path not in fetched


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


@mark.parametrize("testing_counts", (True, False))
@mark.parametrize("use_numpy", (True, False))
def test_histograms(run, client, project_name, testing_counts, use_numpy):
    """Log multiple steps with multiple histograms, and verify that the values are correct. Do this
    once with counts and once with densities, and once with numpy arrays and once with plain lists."""

    path = unique_path("test_histograms")
    value_param = "counts" if testing_counts else "densities"

    logged_steps = []
    # Log a bunch of steps with histograms ranging up to the maximum number of bins
    for step in range(3):
        data = {}
        for i in range(1, 514):
            value = np.array(range(i - 1)) if use_numpy else list(range(i - 1))
            data[f"{path}/hist-{i}"] = Histogram(bin_edges=list(range(i)), **{value_param: value})

        run.log_histograms(data, step=step)
        run.wait_for_processing()
        logged_steps.append(data)

    fetched_histograms = fetch_series_values(
        client, project_name, attributes=logged_steps[0].keys(), custom_run_id=run._run_id
    )

    assert set(fetched_histograms.keys()) == set(logged_steps[0].keys()), "Not all attributes are present"

    # fetched histograms format is: path (str) -> dict of step (float) to histogram dict
    for path, steps in fetched_histograms.items():
        assert len(steps) == len(logged_steps), f"Attribute {path} has incorrect number of steps"

        for step, fetched in steps.items():
            expected = logged_steps[int(step)][path]

            assert fetched["type"] == "COUNTING" if testing_counts else "DENSITY"
            assert fetched["bins_edges"] == expected.bin_edges
            expected_values = expected.counts if testing_counts else expected.densities
            assert fetched["bin_values"] == (expected_values.tolist() if use_numpy else expected_values)
