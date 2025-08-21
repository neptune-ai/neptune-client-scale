import math
import threading
import time
from datetime import (
    datetime,
    timezone,
)

import numpy as np
import pytest

from neptune_scale.api.run import Run
from neptune_scale.util import source_tracking

from .conftest import (
    random_series,
    unique_path,
)
from .test_fetcher import (
    fetch_attribute_values,
    fetch_metric_values,
)

SYNC_TIMEOUT = 60


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
    assert run.wait_for_processing(SYNC_TIMEOUT)

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
    assert run.wait_for_processing(SYNC_TIMEOUT)

    fetched = fetch_attribute_values(client, project_name, custom_run_id=run._run_id, attributes=data.keys())
    for key, value in updated_data.items():
        assert fetched[key] == value, f"The updated value for {key} does not match"


def test_metric(run, client, project_name):
    path = unique_path("test_metric/metric")

    steps, values = random_series()

    for step, value in zip(steps, values):
        run.log_metrics(data={path: value}, step=step)

    assert run.wait_for_processing(SYNC_TIMEOUT)

    fetched = fetch_metric_values(client=client, project=project_name, custom_run_id=run._run_id, attributes=[path])
    assert list(fetched[path].keys()) == steps
    assert list(fetched[path].values()) == values


def test_multiple_metrics(run, client, project_name):
    path_base = unique_path("test_metric/many_metrics")
    data = {f"{path_base}-{i}": i for i in range(20)}

    run.log_metrics(data, step=1)
    assert run.wait_for_processing(SYNC_TIMEOUT)

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

    assert run.wait_for_processing(SYNC_TIMEOUT)

    fetched = fetch_metric_values(client=client, project=project_name, custom_run_id=run._run_id, attributes=[path])
    assert list(fetched[path].keys()) == steps
    assert list(fetched[path].values()) == values

    steps2, values2 = random_series(length=5, start_step=len(steps))

    for step, value in zip(steps2, values2):
        run.log_metrics(data={path: value}, step=step)

    assert run.wait_for_processing(SYNC_TIMEOUT)

    fetched = fetch_metric_values(client=client, project=project_name, custom_run_id=run._run_id, attributes=[path])
    assert list(fetched[path].keys()) == steps + steps2
    assert list(fetched[path].values()) == values + values2


@pytest.mark.skip("Skipped until inf/nan handling is enabled in the backend")
@pytest.mark.parametrize("value", [np.inf, -np.inf, np.nan, math.inf, -math.inf, math.nan])
def test_single_non_finite_metric(run, client, project_name, value):
    path = unique_path("test_series/non_finite")
    step = 1

    run.log_metrics(data={path: value}, step=step)
    assert run.wait_for_processing(SYNC_TIMEOUT)

    fetched = fetch_metric_values(client=client, project=project_name, custom_run_id=run._run_id, attributes=[path])
    assert path in fetched
    if math.isnan(value):
        assert math.isnan(fetched[path][step])
    else:
        assert fetched[path][step] == value


def test_async_lag_callback(api_token, project_name):
    event = threading.Event()
    with Run(
        api_token=api_token,
        project=project_name,
        async_lag_threshold=0.000001,
        on_async_lag_callback=lambda: event.set(),
    ) as run:
        assert run.wait_for_processing(SYNC_TIMEOUT)

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


def test_source_tracking(run, client, project_name):
    # given
    info = source_tracking.read_repository_info(
        path=None, run_command=True, entry_point=True, head_diff=True, upstream_diff=True
    )
    data = {
        "source_code/commit/commit_id": info.commit_id,
        "source_code/commit/message": info.commit_message,
        "source_code/commit/author_name": info.commit_author_name,
        "source_code/commit/author_email": info.commit_author_email,
        "source_code/commit/commit_date": info.commit_date,
        "source_code/branch": info.branch,
        "source_code/remote/origin": info.remotes["origin"],
        "source_code/dirty": info.dirty,
        "source_code/run_command": info.run_command,
    }
    files = {
        "source_code/entry_point": info.entry_point_path,
    }
    diffs = {
        "source_code/diff/head": info.head_diff_content,
        f"source_code/diff/{info.upstream_diff_commit_id}": info.upstream_diff_content,
    }

    # then
    fetched = fetch_attribute_values(
        client, project_name, custom_run_id=run._run_id, attributes=data.keys() | files.keys() | diffs.keys()
    )
    for key, value in data.items():
        assert key in fetched, f"Diff {key} was not fetched, expected value: {value}"
        assert fetched[key] == value, f"Value for {key} does not match"

    for key, value in files.items():
        if value is None:
            # This can happen if the entry point is not available in the test environment
            continue
        assert key in fetched, f"Diff {key} was not fetched, expected content: {value}"
        file_ref = fetched[key]
        assert file_ref["mime_type"] == "text/x-python"
        assert file_ref["size_bytes"] == value.stat().st_size
        assert file_ref["path"] is not None

    for key, value in diffs.items():
        if value is None:
            # This can happen if the upstream diff is not available in the test environment
            continue
        assert key in fetched, f"Diff {key} was not fetched, expected content: {value}"
        file_ref = fetched[key]
        assert file_ref["mime_type"] == "application/patch"
        assert file_ref["size_bytes"] == len(value)
        assert file_ref["path"] is not None
