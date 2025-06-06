from datetime import (
    datetime,
    timedelta,
)
from unittest.mock import (
    Mock,
    call,
)

import pytest
from freezegun import freeze_time
from pytest import (
    fixture,
    mark,
)

from neptune_scale.legacy import (
    Run,
    cleanup_path,
)
from neptune_scale.sync.metadata_splitter import Metrics


@fixture
def run(api_token):
    run = Run(project="dummy/project", mode="offline", api_token=api_token)
    run._log = Mock()
    with run:
        yield run


@fixture
def log_method(run):
    return run._log


@mark.parametrize("value", (1, 1.23, "string", True, False, datetime.now()))
def test_assign_config_atom(run, log_method, value):
    run["path"] = value
    log_method.assert_called_once_with(configs={"path": value})


@mark.parametrize(
    "value, expected",
    (
        ({"foo": 1}, {"base/foo": 1}),
        ({"foo": 1, "bar": 2}, {"base/foo": 1, "base/bar": 2}),
        ({"foo": {"bar": {"baz": 1}, "qux": 2}}, {"base/foo/bar/baz": 1, "base/foo/qux": 2}),
    ),
)
def test_assign_config_dict(run, log_method, value, expected):
    run["base"] = value
    assert log_method.call_count == 1
    assert log_method.call_args.kwargs == {"configs": expected}


@mark.parametrize("value", ({}, {"foo": {}}, {"foo": 1, "bar": {"baz": {}}}))
def test_assign_config_empty_dict(run, value):
    with pytest.raises(ValueError) as exc:
        run["foo"] = value

    exc.match("cannot be empty")


def test_tags(run, log_method):
    run["sys/tags"].add("tag1")
    log_method.assert_called_with(tags_add={"sys/tags": ("tag1",)})

    run["sys/tags"].add(("tag2", "tag3"))
    log_method.assert_called_with(tags_add={"sys/tags": ("tag2", "tag3")})

    run["sys/tags"].remove("tag3")
    log_method.assert_called_with(tags_remove={"sys/tags": ("tag3",)})

    run["sys/tags"].remove(("tag1", "tag2"))
    log_method.assert_called_with(tags_remove={"sys/tags": ("tag1", "tag2")})


def test_append(run, log_method):
    run["sys/series"].append(3, step=1, timestamp=10)
    log_method.assert_called_with(metrics=Metrics(data={"sys/series": 3}), step=1, timestamp=10)

    run["sys/series"].append({"foo": 1, "bar": 2}, step=2)
    log_method.assert_called_with(
        metrics=Metrics(data={"sys/series/foo": 1, "sys/series/bar": 2}), step=2, timestamp=None
    )

    run["my/series"].append({"foo": 1, "bar": 2}, step=3, preview=True, preview_completion=0.3)
    log_method.assert_called_with(
        metrics=Metrics(data={"my/series/foo": 1, "my/series/bar": 2}, preview=True, preview_completion=0.3),
        step=3,
        timestamp=None,
    )


@freeze_time("2024-07-30 12:12:12.000022")
def test_extend(run, log_method):
    now = datetime.now()
    before = now - timedelta(seconds=1)

    run["my/series"].extend([7, 38], steps=[1, 2], timestamps=[before, now])
    log_method.assert_has_calls(
        [
            call(metrics=Metrics(data={"my/series": 7}), step=1, timestamp=before),
            call(metrics=Metrics(data={"my/series": 38}), step=2, timestamp=now),
        ]
    )

    # timestamp defaulting
    run["my/series"].extend([7, 38], steps=[3, 4])
    log_method.assert_has_calls(
        [
            call(metrics=Metrics(data={"my/series": 7}), step=3, timestamp=now),
            call(metrics=Metrics(data={"my/series": 38}), step=4, timestamp=now),
        ]
    )

    # previews
    run["my/series"].extend(
        [7, 38], steps=[5, 6], previews=[False, True], preview_completions=[1.0, 0.5], timestamps=[now, now]
    )
    log_method.assert_has_calls(
        [
            call(metrics=Metrics(data={"my/series": 7}, preview=False, preview_completion=1.0), step=5, timestamp=now),
            call(metrics=Metrics(data={"my/series": 38}, preview=True, preview_completion=0.5), step=6, timestamp=now),
        ]
    )

    # different length of inputs
    with pytest.raises(ValueError):
        run["my/series"].extend([7], steps=[7, 8])


@pytest.mark.parametrize(
    "path", ["", " ", "/", " /", "/ ", "///", "/a ", "/a/b /", "a/b /c", "a /b/c", "a/b/", "a/b ", " /a/b"]
)
def test_cleanup_path_invalid_path(path):
    with pytest.raises(ValueError) as exc:
        cleanup_path(path)

    exc.match("Invalid path:")


@pytest.mark.parametrize(
    "path, expected",
    (
        ("/a/b/c", "a/b/c"),
        ("a a/b/c", "a a/b/c"),
        ("/a a/b/c", "a a/b/c"),
    ),
)
def test_cleanup_path_valid_path(path, expected):
    assert cleanup_path(path) == expected
