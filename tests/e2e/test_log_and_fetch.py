import logging
import math
import os
import pathlib
import re
import threading
import time
from datetime import (
    datetime,
    timezone,
)

import numpy as np
import pytest
from pytest import mark

from neptune_scale.api.run import Run
from neptune_scale.exceptions import NeptuneAttributePathEmpty
from neptune_scale.types import File

from .conftest import (
    random_series,
    unique_path,
)
from .test_fetcher import (
    fetch_attribute_values,
    fetch_files,
    fetch_metric_values,
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


@pytest.mark.parametrize(
    "files",
    [
        {"test_files/file_txt1": b"bytes content"},
        {"test_files/file_txt2": "e2e/resources/file.txt"},
        {"test_files/file_txt3": pathlib.Path("e2e/resources/file.txt")},
        {"test_files/file_txt4": File(source="e2e/resources/file.txt")},
        {"test_files/file_txt5": File(source="e2e/resources/file.txt", mime_type="application/json")},
        {"test_files/file_txt6": File(source=pathlib.Path("e2e/resources/file.txt"), mime_type="application/json")},
        {"test_files/file_txt7": File(source="e2e/resources/file.txt", size=1024)},
        {"test_files/file_txt8": File(source="e2e/resources/file.txt", destination="custom_destination.txt")},
        {"test_files/file_txt9": "e2e/resources/link_file"},
        {"test_files/file_binary1": "e2e/resources/binary_file"},
        {"test_files/file_binary2": pathlib.Path("e2e/resources/binary_file")},
        {"test_files/file_binary3": File(source="e2e/resources/binary_file")},
        {"test_files/file_binary4": File(source="e2e/resources/binary_file", mime_type="audio/mpeg")},
        {"test_files/file_binary5": File(source=pathlib.Path("e2e/resources/binary_file"), mime_type="audio/mpeg")},
        {
            "test_files/file_multiple1a": "e2e/resources/file.txt",
            "test_files/file_multiple1b": "e2e/resources/file.txt",
        },
        {
            "test_files/file_multiple2a": "e2e/resources/file.txt",
            "test_files/file_multiple2b": "e2e/resources/binary_file",
            "test_files/file_multiple2c": b"bytes content",
            "test_files/file_multiple2d": File(source="e2e/resources/file.txt"),
            "test_files/file_multiple2e": File(source=b"bytes content"),
        },
        {"test_files/汉字Пр\U00009999/file_txt2": "e2e/resources/file.txt"},
        {"test_files/file_path_length1-" + "a" * 47: "e2e/resources/file.txt"},  # just below file metadata limit
        {"test_files/file_large1": b"a" * (10 * 1024 * 1024)},
        {"test_files/file_empty1": "e2e/resources/empty_file"},
        {"test_files/file_metadata1": File("e2e/resources/file.txt", mime_type="a" * 128)},
        {"test_files/file_metadata2": File("e2e/resources/file.txt", destination="a" * 800)},
        {"test_files/file_metadata1": File(b"from buffer", mime_type="a" * 128)},
        {"test_files/file_metadata1": File(b"from buffer", destination="a")},
    ],
)
def test_assign_files(caplog, run, client, project_name, run_init_kwargs, temp_dir, files):
    # given
    ensure_test_directory()

    # when
    with caplog.at_level(logging.WARNING):
        run.assign_files(files)

    assert not caplog.records, "No warnings should be logged"

    run.wait_for_processing(SYNC_TIMEOUT)
    # FIXME: We need to account for eventual consistency on the backend. This can be made cleaner.
    time.sleep(10)

    # then
    attributes = list(files.keys())
    fetch_files(
        client,
        project_name,
        custom_run_id=run._run_id,
        attributes_targets={attr: temp_dir / str(i) for i, attr in enumerate(attributes)},
    )

    # check content
    for i, (attribute_path, attribute_content) in enumerate(files.items()):
        compare_content(actual_path=temp_dir / str(i), expected_content=attribute_content)


def test_assign_files_absolute(run, client, project_name, temp_dir):
    # given
    ensure_test_directory()
    # resolve to absolute path only after executing ensure_test_directory
    files = {"test_files/file_txt_absolute1": pathlib.Path("e2e/resources/file.txt").absolute()}

    # when
    run.assign_files(files)
    run.wait_for_processing(SYNC_TIMEOUT)

    # then
    attributes = list(files.keys())
    fetch_files(
        client,
        project_name,
        custom_run_id=run._run_id,
        attributes_targets={attr: temp_dir / attr for attr in attributes},
    )

    # check content
    for attribute_path, attribute_content in files.items():
        compare_content(actual_path=temp_dir / attribute_path, expected_content=attribute_content)


@pytest.mark.parametrize(
    "files, expected",
    [
        (
            {"test_files/file_destination1": b"Hello world"},
            {
                "path": re.compile("[^/]+/test_files_file_destination1-[^/]+/[^/.]+.bin"),
                "size_bytes": 11,
                "mime_type": "application/octet-stream",
            },
        ),
        (
            {"test_files/file_destination2": "e2e/resources/file.txt"},
            {
                "path": re.compile("[^/]+/test_files_file_destination2-[^/]+/file.txt"),
                "size_bytes": 19,
                "mime_type": "text/plain",
            },
        ),
        (
            {"test_files/file_destination3": File(source="e2e/resources/file.txt")},
            {
                "path": re.compile("[^/]+/test_files_file_destination3-[^/]+/file.txt"),
                "size_bytes": 19,
                "mime_type": "text/plain",
            },
        ),
        (
            {
                "test_files/file_destination4": File(
                    source="e2e/resources/file.txt", destination="custom_destination.txt"
                )
            },
            {"path": "custom_destination.txt", "size_bytes": 19, "mime_type": "text/plain"},
        ),
        (
            {"test_files/file_destination5": File(source="e2e/resources/file.txt", size=67)},
            {
                "path": re.compile("[^/]+/test_files_file_destination5-[^/]+/file.txt"),
                "size_bytes": 67,
                "mime_type": "text/plain",
            },
        ),
        (
            {"test_files/file_destination6": File(source="e2e/resources/file.txt", mime_type="application/json")},
            {
                "path": re.compile("[^/]+/test_files_file_destination6-[^/]+/file.txt"),
                "size_bytes": 19,
                "mime_type": "application/json",
            },
        ),
        (
            {"test_files/file_destination7": File(source="e2e/resources/binary_file")},
            {
                "path": re.compile("[^/]+/test_files_file_destination7-[^/]+/binary_file"),
                "size_bytes": 1024,
                "mime_type": "application/octet-stream",
            },
        ),
        (
            {"test_files/file_destination8": File(source="e2e/resources/binary_file", mime_type="audio/mpeg")},
            {
                "path": re.compile("[^/]+/test_files_file_destination8-[^/]+/binary_file"),
                "size_bytes": 1024,
                "mime_type": "audio/mpeg",
            },
        ),
        (
            {"test_files/file_destination9": File(source="e2e/resources/binary_file", size=123)},
            {
                "path": re.compile("[^/]+/test_files_file_destination9-[^/]+/binary_file"),
                "size_bytes": 123,
                "mime_type": "application/octet-stream",
            },
        ),
    ],
)
def test_assign_files_metadata(run, client, project_name, temp_dir, files, expected):
    # given
    ensure_test_directory()

    # when
    run.assign_files(files)
    run.wait_for_processing(SYNC_TIMEOUT)

    # then
    attributes = list(files.keys())
    metadata = fetch_attribute_values(client, project_name, custom_run_id=run._run_id, attributes=attributes)

    for attribute in attributes:
        for key, value in expected.items():
            if isinstance(value, re.Pattern):
                assert re.match(value, metadata[attribute][key])
            else:
                assert metadata[attribute][key] == value


@pytest.mark.parametrize("wait_after_first_upload", [True, False])
def test_assign_files_duplicate_attribute_path(run, client, project_name, temp_dir, wait_after_first_upload):
    # given
    ensure_test_directory()
    files = {"test_files/file_duplicate1": "e2e/resources/file.txt"}

    # when
    run.assign_files(files)
    if wait_after_first_upload:
        run.wait_for_processing(SYNC_TIMEOUT)

    files = {"test_files/file_duplicate1": "e2e/resources/binary_file"}
    run.assign_files(files)
    run.wait_for_processing(SYNC_TIMEOUT)

    # then
    attributes = list(files.keys())
    fetch_files(
        client,
        project_name,
        custom_run_id=run._run_id,
        attributes_targets={attr: temp_dir / attr for attr in attributes},
    )

    # check content
    for attribute_path, attribute_content in files.items():
        compare_content(actual_path=temp_dir / attribute_path, expected_content=attribute_content)


@pytest.mark.parametrize(
    "files, error_type, warnings",
    [
        ({}, None, []),
        (
            {"test_files/file_error1": ""},
            None,
            ["Skipping file attribute `test_files/file_error1`: Cannot determine mime type for file '.'"],
        ),
        ({"": "e2e/resources/file.txt"}, NeptuneAttributePathEmpty, []),
        (
            {"test_files/file_error3": "e2e/resources"},
            None,
            ["Skipping file attribute `test_files/file_error3`: Cannot determine mime type for file 'e2e/resources'"],
        ),  # tries to upload a directory
        (
            {"test_files/file_error4": "e2e/resources/does-not-exist"},
            None,
            [
                "Error determining mime type for e2e/resources/does-not-exist: [Errno 2] No such file or directory: 'e2e/resources/does-not-exist'"
            ],
        ),
        (
            {"test_files/file_error5": pathlib.Path("e2e/resources/does-not-exist")},
            None,
            [
                "Error determining mime type for e2e/resources/does-not-exist: [Errno 2] No such file or directory: 'e2e/resources/does-not-exist'"
            ],
        ),
        ({"test_files/file_error6" + "a" * 1024: "e2e/resources/file.txt"}, None, ["Field paths must be less than"]),
        (
            {"test_files/file_error7": "e2e/resources/invalid_link_file"},
            None,
            ["Too many levels of symbolic links: 'e2e/resources/invalid_link_file'"],
        ),
        (
            {"test_files/file_error8": File("e2e/resources/file.txt", mime_type="a" * 129)},
            None,
            ["Dropping value. File mime type must be a string of at most 128 characters"],
        ),
        (
            {"test_files/file_error9": File("e2e/resources/file.txt", destination="a" * 801)},
            None,
            ["Dropping value. File destination must be a string of at most 800 characters"],
        ),
    ],
)
def test_assign_files_error(run, client, project_name, temp_dir, on_error_queue, caplog, files, error_type, warnings):
    # given
    ensure_test_directory()

    # when
    with caplog.at_level(logging.WARNING):
        run.assign_files(files)

    run.wait_for_processing(SYNC_TIMEOUT)
    time.sleep(2)

    # then
    attributes = list(files.keys())
    try:
        fetch_files(
            client,
            project_name,
            custom_run_id=run._run_id,
            attributes_targets={attr: temp_dir / attr for attr in attributes},
        )
    except AssertionError:
        pass

    for attribute_path, attribute_content in files.items():
        if attribute_path:
            actual_path = temp_dir / attribute_path
            assert not os.path.exists(actual_path), f"File {actual_path} should not exist"

    if error_type is None:
        assert on_error_queue.empty()
    else:
        assert not on_error_queue.empty()
        actual_error = on_error_queue.get()
        assert isinstance(actual_error, error_type)

    for warning in warnings:
        assert any(
            warning in message for message in caplog.messages
        ), f"Warning '{warning}' not found in logs: {'; '.join(caplog.messages)}"


def test_assign_files_error_no_access(run, client, project_name, temp_dir):
    # given
    ensure_test_directory()
    file_path = temp_dir / "file_no_access"
    with open(file_path, "w") as f:
        f.write("test content")
    os.chmod(file_path, 0o000)  # remove read access
    files = {"test_files/file_no_access": file_path}

    # when
    run.assign_files(files)  # emit a warning and skip only

    # then
    attributes = list(files.keys())
    try:
        fetch_files(
            client,
            project_name,
            custom_run_id=run._run_id,
            attributes_targets={attr: temp_dir / attr for attr in attributes},
        )
    except AssertionError:
        pass

    expected_path = temp_dir / attributes[0]
    assert not os.path.exists(expected_path), f"File {expected_path} should not exist"


def compare_content(actual_path, expected_content):
    assert os.path.exists(actual_path), f"File {actual_path} does not exist"

    with open(actual_path, "rb") as f:
        actual_content = f.read()

    if isinstance(expected_content, File):
        expected_content = expected_content.source

    if not isinstance(expected_content, bytes):
        with open(expected_content, "rb") as f:
            expected_content = f.read()

    assert actual_content == expected_content


def ensure_test_directory():
    if pathlib.Path.cwd().name == "tests":
        return
    elif pathlib.Path.cwd().name == "neptune-client-scale":
        os.chdir("tests")
    else:
        assert False, "Test must be run from the tests directory"
