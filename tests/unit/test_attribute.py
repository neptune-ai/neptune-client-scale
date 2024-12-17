from datetime import datetime
from unittest.mock import Mock

import pytest
from pytest import (
    fixture,
    mark,
)

from neptune_scale.api.attribute import cleanup_path
from neptune_scale.exceptions import NeptuneSeriesStepNonIncreasing
from neptune_scale.legacy import Run


@fixture
def run(api_token):
    run = Run(project="dummy/project", run_id="dummy-run", mode="disabled", api_token=api_token)
    # Mock log to be able to assert calls, but also proxy to the actual method so it does its job
    run._attr_store.log = Mock(side_effect=run._attr_store.log)
    with run:
        yield run


@fixture
def store(run):
    return run._attr_store


@mark.parametrize("value", (1, 1.23, "string", True, False, datetime.now()))
def test_assign_config_atom(run, store, value):
    run["path"] = value
    store.log.assert_called_once_with(configs={"path": value})


@mark.parametrize(
    "value, expected",
    (
        ({"foo": 1}, {"base/foo": 1}),
        ({"foo": 1, "bar": 2}, {"base/foo": 1, "base/bar": 2}),
        ({"foo": {"bar": {"baz": 1}, "qux": 2}}, {"base/foo/bar/baz": 1, "base/foo/qux": 2}),
    ),
)
def test_assign_config_dict(run, store, value, expected):
    run["base"] = value
    assert store.log.call_count == 1
    assert store.log.call_args.kwargs == {"configs": expected}


@mark.parametrize("value", ({}, {"foo": {}}, {"foo": 1, "bar": {"baz": {}}}))
def test_assign_config_empty_dict(run, value):
    with pytest.raises(ValueError) as exc:
        run["foo"] = value

    exc.match("cannot be empty")


def test_tags(run, store):
    run["sys/tags"].add("tag1")
    store.log.assert_called_with(tags_add={"sys/tags": ("tag1",)})

    run["sys/tags"].add(("tag2", "tag3"))
    store.log.assert_called_with(tags_add={"sys/tags": ("tag2", "tag3")})

    run["sys/tags"].remove("tag3")
    store.log.assert_called_with(tags_remove={"sys/tags": ("tag3",)})

    run["sys/tags"].remove(("tag1", "tag2"))
    store.log.assert_called_with(tags_remove={"sys/tags": ("tag1", "tag2")})


def test_series(run, store):
    run["my/series"].append(1, step=1, timestamp=10)
    store.log.assert_called_with(metrics={"my/series": 1}, step=1, timestamp=10)

    run["my/series"].append({"foo": 1, "bar": 2}, step=2)
    store.log.assert_called_with(metrics={"my/series/foo": 1, "my/series/bar": 2}, step=2, timestamp=None)


def test_error_on_non_increasing_step(run):
    run["series"].append(1, step=2)

    # Step lower than previous
    with pytest.raises(NeptuneSeriesStepNonIncreasing):
        run["series"].append(2, step=1)

    # Equal to previous, but different value
    with pytest.raises(NeptuneSeriesStepNonIncreasing):
        run["series"].append(3, step=2)

    # Equal to previous, same value -> should pass
    run["series"].append(1, step=2)

    # None should pass, as it means auto-increment
    run["series"].append(4, step=None)


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
