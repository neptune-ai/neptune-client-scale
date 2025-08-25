import logging
import os
import pathlib
import random
import re
import time
from typing import Any

import pytest

from neptune_scale import Run
from neptune_scale.exceptions import NeptuneAttributePathEmpty
from neptune_scale.sync.files import NO_STEP_PLACEHOLDER
from neptune_scale.types import File

from .test_fetcher import (
    fetch_attribute_values,
    fetch_file_series,
    fetch_files,
    fetch_series_values,
)

SYNC_TIMEOUT = 60


@pytest.mark.parametrize(
    "files",
    [
        {"test_files/file_txt1": b"bytes content"},
        {"test_files/file_txt2": "e2e/resources/file.txt"},
        {"test_files/file_txt3": pathlib.Path("e2e/resources/file.txt")},
        {"test_files/file_txt4": File(source="e2e/resources/file.txt")},
        {"test_files/file_txt5": File(source="e2e/resources/file.txt", mime_type="application/json")},
        {"test_files/file_txt6": File(source=pathlib.Path("e2e/resources/file.txt"), mime_type="application/json")},
        {"test_files/file_txt7": File(source="e2e/resources/file.txt", size=19)},
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
        {"test_files/file_large1": random.randbytes(10 * 1024 * 1024)},
        {
            "test_files/file_large2": File(
                "\n".join(chr(ord("a") + line) * 1024 * 1023 for line in range(11)).encode("utf8"),
                mime_type="text/plain",
            )
        },
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
    extra_wait()

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
                "path": re.compile("[^/]+/test_files_file_destination1-[^/]+/" + NO_STEP_PLACEHOLDER + "/[^/.]+.bin"),
                "size_bytes": 11,
                "mime_type": "application/octet-stream",
            },
        ),
        (
            {"test_files/file_destination2": "e2e/resources/file.txt"},
            {
                "path": re.compile("[^/]+/test_files_file_destination2-[^/]+/" + NO_STEP_PLACEHOLDER + "/file.txt"),
                "size_bytes": 19,
                "mime_type": "text/plain",
            },
        ),
        (
            {"test_files/file_destination3": File(source="e2e/resources/file.txt")},
            {
                "path": re.compile("[^/]+/test_files_file_destination3-[^/]+/" + NO_STEP_PLACEHOLDER + "/file.txt"),
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
                "path": re.compile("[^/]+/test_files_file_destination5-[^/]+/" + NO_STEP_PLACEHOLDER + "/file.txt"),
                "size_bytes": 67,
                "mime_type": "text/plain",
            },
        ),
        (
            {"test_files/file_destination6": File(source="e2e/resources/file.txt", mime_type="application/json")},
            {
                "path": re.compile("[^/]+/test_files_file_destination6-[^/]+/" + NO_STEP_PLACEHOLDER + "/file.txt"),
                "size_bytes": 19,
                "mime_type": "application/json",
            },
        ),
        (
            {"test_files/file_destination7": File(source="e2e/resources/binary_file")},
            {
                "path": re.compile("[^/]+/test_files_file_destination7-[^/]+/" + NO_STEP_PLACEHOLDER + "/binary_file"),
                "size_bytes": 1024,
                "mime_type": "application/octet-stream",
            },
        ),
        (
            {"test_files/file_destination8": File(source="e2e/resources/binary_file", mime_type="audio/mpeg")},
            {
                "path": re.compile("[^/]+/test_files_file_destination8-[^/]+/" + NO_STEP_PLACEHOLDER + "/binary_file"),
                "size_bytes": 1024,
                "mime_type": "audio/mpeg",
            },
        ),
        (
            {"test_files/file_destination9": File(source="e2e/resources/binary_file", size=123)},
            {
                "path": re.compile("[^/]+/test_files_file_destination9-[^/]+/" + NO_STEP_PLACEHOLDER + "/binary_file"),
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
    extra_wait()

    # then
    attributes = list(files.keys())
    fetch_files(
        client,
        project_name,
        custom_run_id=run._run_id,
        attributes_targets={attr: temp_dir / attr for attr in attributes},
    )

    assert not os.listdir(temp_dir)

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
    fetch_files(
        client,
        project_name,
        custom_run_id=run._run_id,
        attributes_targets={attr: temp_dir / attr for attr in attributes},
    )

    expected_path = temp_dir / attributes[0]
    assert not os.path.exists(expected_path), f"File {expected_path} should not exist"


@pytest.mark.parametrize(
    "files",
    [
        {"test_file_series/file_txt1": b"bytes content"},
        {"test_file_series/file_txt2": "e2e/resources/file.txt"},
        {"test_file_series/file_txt3": pathlib.Path("e2e/resources/file.txt")},
        {"test_file_series/file_txt4": File(source="e2e/resources/file.txt")},
        {"test_file_series/file_txt5": File(source="e2e/resources/file.txt", mime_type="application/json")},
        {
            "test_file_series/file_txt6": File(
                source=pathlib.Path("e2e/resources/file.txt"), mime_type="application/json"
            )
        },
        {"test_file_series/file_txt7": File(source="e2e/resources/file.txt", size=19)},
        {"test_file_series/file_txt8": File(source="e2e/resources/file.txt", destination="custom_destination.txt")},
        {"test_file_series/file_txt9": "e2e/resources/link_file"},
        {"test_file_series/file_binary1": "e2e/resources/binary_file"},
        {"test_file_series/file_binary2": pathlib.Path("e2e/resources/binary_file")},
        {"test_file_series/file_binary3": File(source="e2e/resources/binary_file")},
        {"test_file_series/file_binary4": File(source="e2e/resources/binary_file", mime_type="audio/mpeg")},
        {
            "test_file_series/file_binary5": File(
                source=pathlib.Path("e2e/resources/binary_file"), mime_type="audio/mpeg"
            )
        },
        {
            "test_file_series/file_multiple1a": "e2e/resources/file.txt",
            "test_file_series/file_multiple1b": "e2e/resources/file.txt",
        },
        {
            "test_file_series/file_multiple2a": "e2e/resources/file.txt",
            "test_file_series/file_multiple2b": "e2e/resources/binary_file",
            "test_file_series/file_multiple2c": b"bytes content",
            "test_file_series/file_multiple2d": File(source="e2e/resources/file.txt"),
            "test_file_series/file_multiple2e": File(source=b"bytes content"),
        },
        {"test_file_series/汉字Пр\U00009999/file_txt2": "e2e/resources/file.txt"},
        {"test_file_series/file_path_length1-" + "a" * 47: "e2e/resources/file.txt"},  # just below file metadata limit
        {"test_file_series/file_large1": random.randbytes(10 * 1024 * 1024)},
        {"test_file_series/file_empty1": "e2e/resources/empty_file"},
        {"test_file_series/file_metadata1": File("e2e/resources/file.txt", mime_type="a" * 128)},
        {"test_file_series/file_metadata2": File("e2e/resources/file.txt", destination="a" * 800)},
        {"test_file_series/file_metadata3": File(b"from buffer", mime_type="a" * 128)},
        {"test_file_series/file_metadata4": File(b"from buffer", destination="a")},
    ],
)
def test_log_files_single(caplog, run, client, project_name, run_init_kwargs, temp_dir, files):
    # given
    ensure_test_directory()
    step = 1.0

    # when
    with caplog.at_level(logging.WARNING):
        run.log_files(files=files, step=step)

    assert not caplog.records, "No warnings should be logged"

    run.wait_for_processing(SYNC_TIMEOUT)
    extra_wait()

    # then
    attributes = list(files.keys())
    fetch_file_series(
        client,
        project_name,
        custom_run_id=run._run_id,
        attributes_targets={attr: temp_dir / str(i) for i, attr in enumerate(attributes)},
    )

    # check content
    for i, (attribute_path, attribute_content) in enumerate(files.items()):
        compare_content(actual_path=temp_dir / str(i) / f"{step:19.6f}", expected_content=attribute_content)


@pytest.mark.parametrize(
    "files, expected",
    [
        (
            {"test_file_series/file_destination1": b"Hello world"},
            {
                "path": re.compile("[^/]+/test_file_series_file_destination1-[^/]+/000000000002_000000/[^/.]+.bin"),
                "size_bytes": 11,
                "mime_type": "application/octet-stream",
            },
        ),
        (
            {"test_file_series/file_destination2": "e2e/resources/file.txt"},
            {
                "path": re.compile("[^/]+/test_file_series_file_destination2-[^/]+/000000000002_000000/file.txt"),
                "size_bytes": 19,
                "mime_type": "text/plain",
            },
        ),
        (
            {"test_file_series/file_destination3": File(source="e2e/resources/file.txt")},
            {
                "path": re.compile("[^/]+/test_file_series_file_destination3-[^/]+/000000000002_000000/file.txt"),
                "size_bytes": 19,
                "mime_type": "text/plain",
            },
        ),
        (
            {
                "test_file_series/file_destination4": File(
                    source="e2e/resources/file.txt", destination="custom_destination.txt"
                )
            },
            {"path": "custom_destination.txt", "size_bytes": 19, "mime_type": "text/plain"},
        ),
        (
            {"test_file_series/file_destination5": File(source="e2e/resources/file.txt", size=67)},
            {
                "path": re.compile("[^/]+/test_file_series_file_destination5-[^/]+/000000000002_000000/file.txt"),
                "size_bytes": 67,
                "mime_type": "text/plain",
            },
        ),
        (
            {"test_file_series/file_destination6": File(source="e2e/resources/file.txt", mime_type="application/json")},
            {
                "path": re.compile("[^/]+/test_file_series_file_destination6-[^/]+/000000000002_000000/file.txt"),
                "size_bytes": 19,
                "mime_type": "application/json",
            },
        ),
        (
            {"test_file_series/file_destination7": File(source="e2e/resources/binary_file")},
            {
                "path": re.compile("[^/]+/test_file_series_file_destination7-[^/]+/000000000002_000000/binary_file"),
                "size_bytes": 1024,
                "mime_type": "application/octet-stream",
            },
        ),
        (
            {"test_file_series/file_destination8": File(source="e2e/resources/binary_file", mime_type="audio/mpeg")},
            {
                "path": re.compile("[^/]+/test_file_series_file_destination8-[^/]+/000000000002_000000/binary_file"),
                "size_bytes": 1024,
                "mime_type": "audio/mpeg",
            },
        ),
        (
            {"test_file_series/file_destination9": File(source="e2e/resources/binary_file", size=123)},
            {
                "path": re.compile("[^/]+/test_file_series_file_destination9-[^/]+/000000000002_000000/binary_file"),
                "size_bytes": 123,
                "mime_type": "application/octet-stream",
            },
        ),
    ],
)
def test_log_files_single_metadata(run, client, project_name, temp_dir, files, expected):
    # given
    ensure_test_directory()
    step = 2.0

    # when
    run.log_files(files, step=step)
    run.wait_for_processing(SYNC_TIMEOUT)

    # then
    attributes = list(files.keys())
    metadata = fetch_series_values(client, project_name, custom_run_id=run._run_id, attributes=attributes)

    for attribute in attributes:
        for step, step_metadata in metadata[attribute].items():
            assert step == 2.0
            for key, value in expected.items():
                if isinstance(value, re.Pattern):
                    assert re.match(value, step_metadata[key])
                else:
                    assert step_metadata[key] == value


@pytest.mark.parametrize(
    "files, error_type, warnings",
    [
        ({}, None, []),
        (
            {"test_file_series/file_error1": ""},
            None,
            ["Skipping file attribute `test_file_series/file_error1`: Cannot determine mime type for file '.'"],
        ),
        ({"": "e2e/resources/file.txt"}, NeptuneAttributePathEmpty, []),
        (
            {"test_file_series/file_error3": "e2e/resources"},
            None,
            [
                "Skipping file attribute `test_file_series/file_error3`: Cannot determine mime type for file 'e2e/resources'"
            ],
        ),  # tries to upload a directory
        (
            {"test_file_series/file_error4": "e2e/resources/does-not-exist"},
            None,
            [
                "Error determining mime type for e2e/resources/does-not-exist: [Errno 2] No such file or directory: 'e2e/resources/does-not-exist'"
            ],
        ),
        (
            {"test_file_series/file_error5": pathlib.Path("e2e/resources/does-not-exist")},
            None,
            [
                "Error determining mime type for e2e/resources/does-not-exist: [Errno 2] No such file or directory: 'e2e/resources/does-not-exist'"
            ],
        ),
        (
            {"test_file_series/file_error6" + "a" * 1024: "e2e/resources/file.txt"},
            None,
            ["Field paths must be less than"],
        ),
        (
            {"test_file_series/file_error7": "e2e/resources/invalid_link_file"},
            None,
            ["Too many levels of symbolic links: 'e2e/resources/invalid_link_file'"],
        ),
        (
            {"test_file_series/file_error8": File("e2e/resources/file.txt", mime_type="a" * 129)},
            None,
            ["Dropping value. File mime type must be a string of at most 128 characters"],
        ),
        (
            {"test_file_series/file_error9": File("e2e/resources/file.txt", destination="a" * 801)},
            None,
            ["Dropping value. File destination must be a string of at most 800 characters"],
        ),
    ],
)
def test_log_files_error_single(
    run, client, project_name, temp_dir, on_error_queue, caplog, files, error_type, warnings
):
    # given
    ensure_test_directory()
    step = 3.0

    # when
    with caplog.at_level(logging.WARNING):
        run.log_files(files, step=step)

    run.wait_for_processing(SYNC_TIMEOUT)
    time.sleep(2)

    # then
    attributes = list(files.keys())
    fetch_file_series(
        client,
        project_name,
        custom_run_id=run._run_id,
        attributes_targets={attr: temp_dir / attr for attr in attributes},
    )
    assert not os.listdir(temp_dir)

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


@pytest.mark.parametrize(
    "file_series",
    [
        [
            (1.0, {"test_file_series/series_1": b"bytes content 1"}),
            (2.0, {"test_file_series/series_1": b"bytes content 2"}),
            (3.0, {"test_file_series/series_1": b"bytes content 3"}),
        ],
        [
            (
                1.0,
                {
                    "test_file_series/series_2a": b"bytes content 1",
                    "test_file_series/series_2b": b"bytes content 2",
                },
            ),
        ],
        [
            (
                1.0,
                {
                    "test_file_series/series_3a": b"bytes content 1",
                    "test_file_series/series_3b": b"bytes content 2",
                },
            ),
            (
                3.0,
                {
                    "test_file_series/series_3b": b"bytes content 3",
                    "test_file_series/series_3c": b"bytes content 4",
                },
            ),
        ],
        [
            (1.0, {"test_file_series/series_4": b"bytes content"}),
            (1.1, {"test_file_series/series_4": "e2e/resources/file.txt"}),
            (1.2, {"test_file_series/series_4": pathlib.Path("e2e/resources/file.txt")}),
            (1.3, {"test_file_series/series_4": File(source="e2e/resources/file.txt")}),
        ],
        [
            (0.0, {"test_file_series/series_5": b"bytes content 1"}),
            (0.1**6, {"test_file_series/series_5": b"bytes content 2"}),
            (1.0, {"test_file_series/series_5": b"bytes content 3"}),
            (1.000001, {"test_file_series/series_5": b"bytes content 4"}),
            (10**11, {"test_file_series/series_5": b"bytes content 5"}),
            (9 * 10**11, {"test_file_series/series_5": b"bytes content 6"}),
            (
                999999999999.9999,
                {"test_file_series/series_5": b"bytes content 7"},
            ),  # we run into float precision issues
        ],
        [
            (1.0, {"test_file_series/series_6a": b"bytes content a 1"}),
            (2.0, {"test_file_series/series_6a": b"bytes content a 2"}),
            (1.0, {"test_file_series/series_6b": b"bytes content b 1"}),
            (3.0, {"test_file_series/series_6a": b"bytes content a 3"}),
            (2.0, {"test_file_series/series_6b": b"bytes content b 2"}),
        ],
        [
            (3.0, {"test_file_series/series_7": b"bytes content 1"}),
            (2.0, {"test_file_series/series_7": b"bytes content 2"}),
            (1.0, {"test_file_series/series_7": b"bytes content 3"}),
        ],
        [
            (1.0, {"test_file_series/series_8": b"bytes content 1"}),
            (1.0, {"test_file_series/series_8": b"bytes content 2"}),
        ],
    ],
)
def test_log_files_multiple(caplog, run, client, project_name, run_init_kwargs, temp_dir, file_series):
    # given
    ensure_test_directory()
    attribute_series: dict[str, dict[float, Any]] = {}
    for step, files in file_series:
        for attr, content in files.items():
            attribute_series.setdefault(attr, {})[step] = content

    # when
    with caplog.at_level(logging.WARNING):
        for step, files in file_series:
            run.log_files(files=files, step=step)

    assert not caplog.records, "No warnings should be logged"

    run.wait_for_processing(SYNC_TIMEOUT)
    extra_wait()

    # then
    fetch_file_series(
        client,
        project_name,
        custom_run_id=run._run_id,
        attributes_targets={attr: temp_dir / str(i) for i, attr in enumerate(attribute_series.keys())},
    )

    for i, attribute_series in enumerate(attribute_series.values()):
        for step, content in attribute_series.items():
            compare_content(actual_path=temp_dir / str(i) / f"{step:19.6f}", expected_content=content)


def test_log_files_in_chunks(monkeypatch, client, project_name, run_init_kwargs, temp_dir):
    """Test downloading files larger than the chunk size."""

    monkeypatch.setenv("NEPTUNE_FILE_UPLOAD_CHUNK_SIZE", str(1024 * 1024))
    ensure_test_directory()
    file_data = random.randbytes(5 * 1024 * 1024)

    run = Run(**run_init_kwargs)
    run.assign_files({"test_files/test_chunks": file_data})
    run.wait_for_processing(SYNC_TIMEOUT)
    extra_wait()

    fetch_files(
        client,
        project_name,
        custom_run_id=run._run_id,
        attributes_targets={"test_files/test_chunks": temp_dir / "test_chunks"},
    )

    compare_content(temp_dir / "test_chunks", file_data)


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


def extra_wait(timeout=10):
    # FIXME: We need to account for eventual consistency on the backend. This can be made cleaner.
    # In other words, our wait for file upload is not enough. The file is not immediately available after the upload.
    time.sleep(timeout)
