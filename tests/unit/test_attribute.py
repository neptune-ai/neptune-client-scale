from datetime import datetime
from unittest.mock import Mock

import pytest
from pytest import (
    fixture,
    mark,
)

from neptune_scale import Run


@fixture
def run(api_token):
    run = Run(project="dummy/project", run_id="dummy-run", mode="disabled", api_token=api_token)
    run._attr_store.log = Mock()
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
    run["sys/series"].append(1, step=1, timestamp=10)
    store.log.assert_called_with(metrics={"sys/series": 1}, step=1, timestamp=10)

    run["sys/series"].append({"foo": 1, "bar": 2}, step=2)
    store.log.assert_called_with(metrics={"sys/series/foo": 1, "sys/series/bar": 2}, step=2, timestamp=None)
