import tempfile
from pathlib import Path
from unittest.mock import Mock

import pytest
from pytest import fixture

from neptune_scale import Run
from neptune_scale.sync.parameters import MAX_FILE_UPLOAD_BUFFER_SIZE


@fixture
def run(api_token):
    run = Run(project="workspace/project", api_token=api_token, run_id="run_id", mode="disabled")
    run._attr_store.upload_file = Mock()

    return run


def test_data_and_path_arguments(run):
    with pytest.raises(ValueError) as exc:
        run.log_file("file.txt")

    exc.match("Either `path` or `data` must be provided")

    with pytest.raises(ValueError) as exc:
        run.log_file("file.txt", data=b"", path="/some/file.txt")

    exc.match("Only one of `path` or `data` can be provided")


def test_too_large_data(run):
    with pytest.raises(ValueError) as exc:
        run.log_file("file.txt", data=b"a" * MAX_FILE_UPLOAD_BUFFER_SIZE + b"foo")

    exc.match("must not exceed")


def test_file_upload_not_a_file(run):
    with pytest.raises(ValueError) as exc, tempfile.TemporaryDirectory() as temp_dir:
        run.log_file("file.txt", path=temp_dir)

    exc.match("is not a file")


def test_file_upload_file_does_not_exist(run):
    with pytest.raises(FileNotFoundError) as exc:
        run.log_file("file.txt", path="/does/not/exist")

    exc.match("does not exist")


def test_file_upload_with_data(run):
    run.log_file("file.txt", data=b"foo")

    run._attr_store.upload_file.assert_called_once_with(
        attribute_path="file.txt",
        data=b"foo",
        local_path=None,
        target_basename=None,
        target_path=None,
        timestamp=None,
    )


def test_file_upload_with_local_file(run):
    with tempfile.NamedTemporaryFile() as temp_file:
        temp_file.write(b"foo")
        temp_file.flush()

        run.log_file("file.txt", path=temp_file.name)

        run._attr_store.upload_file.assert_called_once_with(
            attribute_path="file.txt",
            data=None,
            local_path=Path(temp_file.name),
            target_basename=None,
            target_path=None,
            timestamp=None,
        )
