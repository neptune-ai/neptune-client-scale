import tempfile
from datetime import datetime
from pathlib import Path
from queue import Empty
from unittest.mock import (
    Mock,
    patch,
)

import pytest
from pytest import (
    fixture,
    mark,
)

from neptune_scale.exceptions import NeptuneScaleError
from neptune_scale.sync.errors_tracking import ErrorsQueue
from neptune_scale.sync.files.queue import FileUploadQueue
from neptune_scale.sync.files.worker import (
    FileUploadWorkerThread,
    determine_path_and_mime_type,
)


@fixture
def queue():
    return FileUploadQueue()


@fixture
def errors_queue():
    return ErrorsQueue()


@fixture
def worker(queue, api_token, errors_queue):
    worker = FileUploadWorkerThread(
        project="project",
        run_id="run_id",
        api_token=api_token,
        family="family",
        input_queue=queue,
        errors_queue=errors_queue,
        mode="disabled",
    )

    worker._request_upload_url = Mock(return_value="http://DUMMY.localhost/")
    worker._submit_attribute = Mock()
    worker._wait_for_completion = Mock()

    worker.start()

    return worker


@mark.parametrize(
    "local, full, basename, expected",
    (
        ("some/file.py", None, None, "RUN/ATTR/UUID4/file.py"),
        ("some/file.py", None, "file.txt", "RUN/ATTR/file.txt"),
        ("some/file.py", "full/path.txt", None, "full/path.txt"),
        ("some/file.py", "full/path.txt", "basename", "full/path.txt"),
    ),
)
def test_determine_path(local, full, basename, expected):
    with patch("uuid.uuid4", return_value="UUID4"):
        path, mimetype = determine_path_and_mime_type("RUN", "ATTR", Path(local), full, basename)
        assert path == expected


@mark.parametrize(
    "attr, local, expected",
    (
        ("attr", None, "application/octet-stream"),
        ("attr.jpg", None, "image/jpeg"),
        ("attr.jpg", Path("local/file.py"), "text/x-python"),
        ("attr.jpg", Path("local/file"), "image/jpeg"),
    ),
)
def test_determine_mime_type(attr, local, expected):
    path, mimetype = determine_path_and_mime_type("RUN", attr, local, None, None)
    assert mimetype == expected


def test_queue_wait_for_completion(queue):
    queue.submit(
        attribute_path="attr",
        local_path=None,
        data=b"test",
        target_path=None,
        target_basename=None,
        timestamp=datetime.now(),
    )
    queue.submit(
        attribute_path="attr2",
        local_path=None,
        data=b"test",
        target_path=None,
        target_basename=None,
        timestamp=datetime.now(),
    )

    assert queue.active_uploads == 2

    queue.decrement_active()
    assert queue.active_uploads == 1

    assert not queue.wait_for_completion(timeout=0.5)

    queue.decrement_active()
    assert queue.active_uploads == 0

    assert queue.wait_for_completion(timeout=1)


def test_successful_upload_from_buffer(worker, queue, errors_queue):
    data = b"test"

    def expect_bytes(source, _url, _mime_type):
        assert source.read() == data

    with patch("neptune_scale.sync.files.worker.upload_file", Mock(side_effect=expect_bytes)) as upload_file:
        queue.submit(
            attribute_path="attr.txt",
            local_path=None,
            data=data,
            target_path=None,
            target_basename=None,
            timestamp=datetime.now(),
        )
        assert queue.wait_for_completion(timeout=10)
        assert queue.active_uploads == 0

        worker.close()

        worker._request_upload_url.assert_called_once()
        worker._submit_attribute.assert_called_once()
        worker._wait_for_completion.assert_called_once()

        upload_file.assert_called_once()
        with pytest.raises(Empty):
            errors_queue.get(timeout=1)


def test_successful_upload_from_file(worker, queue, errors_queue):
    data = b"test"

    def expect_bytes(source, _url, _mime_type):
        assert source.read() == data

    with (
        patch("neptune_scale.sync.files.worker.upload_file", Mock(side_effect=expect_bytes)) as upload_file,
        tempfile.NamedTemporaryFile("w+b") as temp_file,
    ):
        temp_file.write(data)
        temp_file.flush()

        queue.submit(
            attribute_path="attr.txt",
            local_path=Path(temp_file.name),
            data=None,
            target_path=None,
            target_basename=None,
            timestamp=datetime.now(),
        )
        assert queue.wait_for_completion(timeout=10)
        assert queue.active_uploads == 0

        worker.close()

    worker._request_upload_url.assert_called_once()
    worker._submit_attribute.assert_called_once()
    worker._wait_for_completion.assert_called_once()

    upload_file.assert_called_once()
    with pytest.raises(Empty):
        errors_queue.get(timeout=1)


def test_file_does_not_exist(worker, queue, errors_queue):
    queue.submit(
        attribute_path="attr.txt",
        local_path=Path("/does/not/exist"),
        data=None,
        target_path=None,
        target_basename=None,
        timestamp=datetime.now(),
    )
    assert queue.wait_for_completion(timeout=10)
    assert queue.active_uploads == 0

    assert isinstance(errors_queue.get(timeout=1), FileNotFoundError)


def test_upload_error(worker, queue, errors_queue):
    """Trigger an error in upload_file and check if the error is propagated to the errors_queue."""
    error = NeptuneScaleError(message="This error is expected to happen")

    with patch("neptune_scale.sync.files.worker.upload_file", Mock(side_effect=error)) as upload_file:
        queue.submit(
            attribute_path="attr.txt",
            local_path=None,
            data=b"",
            target_path=None,
            target_basename=None,
            timestamp=datetime.now(),
        )
        assert queue.wait_for_completion(timeout=10)
        assert queue.active_uploads == 0

    worker.close()

    worker._request_upload_url.assert_called_once()
    worker._submit_attribute.assert_not_called()
    worker._wait_for_completion.assert_not_called()

    upload_file.assert_called_once()
    assert errors_queue.get(timeout=1).message == error.message
