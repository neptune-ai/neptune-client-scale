import math
import os
import pathlib
import threading
import time
from datetime import (
    datetime,
    timezone,
)

import numpy as np
import pytest
from neptune_fetcher import ReadOnlyRun
from neptune_fetcher.alpha import runs, filters
from pytest import mark

from neptune_scale.api.run import Run
from neptune_scale.api.types import File

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

@pytest.mark.parametrize(
    "files",
    [
        {"test_files/file_txt1": b"bytes content"},
        {"test_files/file_txt2": "tests/e2e/resources/file.txt"},
        {"test_files/file_txt3": pathlib.Path("tests/e2e/resources/file.txt")},
        {"test_files/file_txt4": pathlib.Path("tests/e2e/resources/file.txt").absolute()},
        {"test_files/file_txt5": File(source="tests/e2e/resources/file.txt")},
        {"test_files/file_txt6": File(source="tests/e2e/resources/file.txt", mime_type="application/json")},
        {"test_files/file_txt7": File(source=pathlib.Path("tests/e2e/resources/file.txt"), mime_type="application/json")},
        {"test_files/file_txt8": "tests/e2e/resources/link_file"},
        {"test_files/file_binary1": "tests/e2e/resources/binary_file"},
        {"test_files/file_binary2": pathlib.Path("tests/e2e/resources/binary_file")},
        {"test_files/file_binary3": File(source="tests/e2e/resources/binary_file")},
        {"test_files/file_binary4": File(source="tests/e2e/resources/binary_file", mime_type="audio/mpeg")},
        {"test_files/file_binary5": File(source=pathlib.Path("tests/e2e/resources/binary_file"), mime_type="audio/mpeg")},
        {"test_files/file_multiple1a": "tests/e2e/resources/file.txt", "test_files/file_multiple1b": "tests/e2e/resources/file.txt"},
        {"test_files/file_multiple2a": "tests/e2e/resources/file.txt", "test_files/file_multiple2b": "tests/e2e/resources/binary_file", "test_files/file_multiple2c": b"bytes content"},
        {"test_files/汉字Пр\U00009999/file_txt2": "tests/e2e/resources/file.txt"},  # todo: are there invalid characters? add an error case
        {"test_files/file_path_length1" + "a" * 3 * 1024: "tests/e2e/resources/text_file.txt"},  # just below file metadata limit
        {"test_files/file_large1": b"a" * (10 * 1024 * 1024)},
        {"test_files/file_empty1": "tests/e2e/resources/empty_file"}
    ],
)
def test_assign_files(run, run_init_kwargs, temp_dir, files):
    # when
    run.assign_files(files)
    run.wait_for_processing(SYNC_TIMEOUT)

    # then
    attributes = list(files.keys())
    runs.download_files(runs=run_init_kwargs["run_id"], attributes=filters.AttributeFilter(name_eq=attributes), destination=temp_dir)

    # check content
    for attribute, expected in files.items():
        file_path = os.path.join(temp_dir, attribute)
        assert os.path.exists(file_path), f"File {file_path} does not exist"
        if isinstance(expected, bytes):
            with open(file_path, "rb") as f:
                content = f.read()
            assert content == expected, f"Content of {file_path} does not match"
        else:
            with open(file_path, "r") as f:
                content = f.read()
            with open(expected, "r") as f:
                expected_content = f.read()
            assert content == expected_content, f"Content of {file_path} does not match"


@pytest.mark.parametrize(
    "files",
    [
        {"test_files/file_destination1": File(source="tests/e2e/resources/file.txt", destination="custom_destination.txt")},
    ]
)
def test_assign_files_destination(run, run_init_kwargs, temp_dir, files):
    # when
    run.assign_files(files)
    run.wait_for_processing(SYNC_TIMEOUT)

    # then
    attributes = list(files.keys())
    df = runs.fetch_runs_table(runs=run_init_kwargs["run_id"], attributes=filters.AttributeFilter(name_eq=attributes))
    df[run_init_kwargs["run_id"]["test_files/file_destination1"] == "custom_destination.txt"]

@pytest.mark.parametrize(
    "files",
    [
        {},
        {"test_files/file_error1": ""},
        {"": "tests/e2e/resources/text_file.txt"},
        {"test_files/file_error3": "tests/e2e/resources"},  # a directory
        {"test_files/file_error4": "tests/e2e/resources/does-not-exist"},
        {"test_files/file_error5": pathlib.Path("tests/e2e/resources/does-not-exist")},
        {"test_files/file_error6" + "a" * 4 * 1024: "tests/e2e/resources/text_file.txt"},  # file metadata limit
        {"test_files/file_error_link": "tests/e2e/resources/invalid_link_file"},
        {"test_files/file_error_large1": b"a" * (20 * 1024 * 1024)},  # is there a size limit?
    ],
)
def test_assign_files_error(run, run_init_kwargs, temp_dir, files):
    with pytest.raises(ValueError):
        run.assign_files(files)


def test_assign_files_error_no_access(run, run_init_kwargs, temp_dir):
    # when
    file_path = temp_dir / "file_no_access"
    with open(file_path, "w") as f:
        f.write("test content")
    os.chmod(file_path, 0o000)  # remove read access

    # then
    files = {"test_files/file_no_access": file_path}
    with pytest.raises(ValueError):
        run.assign_files(files)

def test_assign_files_duplicate(run, run_init_kwargs, temp_dir):
    files = {"test_files/file_duplicate1": "tests/e2e/resources/text_file.txt"}

    run.assign_files(files)
    run.assign_files(files)
    run.wait_for_processing(SYNC_TIMEOUT)

    runs.download_files(runs=run_init_kwargs["run_id"], attributes="test_files/file_duplicate1", destination=temp_dir)
