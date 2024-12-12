from pathlib import Path
from unittest.mock import patch

from pytest import mark

from neptune_scale.sync.files.worker import determine_path_and_mime_type


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
