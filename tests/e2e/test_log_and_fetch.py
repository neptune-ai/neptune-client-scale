import logging
import math
import os
import threading
import time
from datetime import (
    datetime,
    timezone,
)
from unittest.mock import patch

import numpy as np
from pytest import mark

from neptune_scale.api.run import Run
from neptune_scale.cli.sync import SyncRunner
from neptune_scale.util import get_logger

from .conftest import (
    random_series,
    unique_path, sleep_3s,
)
from .test_fetcher import (
    fetch_attribute_values,
    fetch_metric_values,
)
from .test_sync import API_TOKEN

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
SYNC_TIMEOUT = 30

logger = get_logger()


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
    run.wait_for_processing(SYNC_TIMEOUT)

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


@mark.parametrize("value", [np.inf, -np.inf, np.nan, math.inf, -math.inf, math.nan])
def test_single_non_finite_metric(run, client, project_name, value):
    path = unique_path("test_series/non_finite")

    run.log_metrics(data={path: value}, step=1)
    assert run.wait_for_processing(SYNC_TIMEOUT)

    fetched = fetch_metric_values(client=client, project=project_name, custom_run_id=run._run_id, attributes=[path])
    assert path not in fetched


def test_async_lag_callback():
    event = threading.Event()
    with Run(
        project=NEPTUNE_PROJECT,
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


def test_concurrent(client, project_name, run_init_kwargs):
    logger.info("Phase 1") ###

    def in_thread():
        with Run(mode="offline") as run_1:
            db_path = run_1._operations_repo._db_path
            for i in range(10_000):
                run_1.log_configs(data={f"int-value-{i}": i})

        runner = SyncRunner(api_token=API_TOKEN, run_log_file=db_path)
        runner.start()
        time.sleep(2)

    thread = threading.Thread(target=in_thread)
    thread.start()
    thread.join(timeout=SYNC_TIMEOUT)

    logger.info("Phase 2") ###

    run_2 = Run(resume=True)
    run_2._sync_process.terminate()
    run_2._sync_process.join()

    for i in range(5):
        run_2.log_configs({f"test_concurrent/int-value-{i}": i * 2 + 1})

    time.sleep(5)

    logger.info("Phase 3") ###
    assert not run_2.wait_for_processing(SYNC_TIMEOUT)
