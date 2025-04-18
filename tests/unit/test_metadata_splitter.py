import math
from datetime import datetime
from unittest.mock import patch

import numpy as np
import pytest
from freezegun import freeze_time
from google.protobuf.timestamp_pb2 import Timestamp
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
    SET_OPERATION,
    ModifySet,
    ModifyStringSet,
    Preview,
    Step,
    StringSet,
    UpdateRunSnapshot,
    Value,
)
from pytest import mark

from neptune_scale.api.metrics import Metrics
from neptune_scale.exceptions import (
    NeptuneFloatValueNanInfUnsupported,
    NeptuneUnableToLogData,
)
from neptune_scale.sync.metadata_splitter import (
    FileRefData,
    MetadataSplitter,
)


@freeze_time("2024-07-30 12:12:12.000022")
def test_empty():
    # given
    builder = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        timestamp=datetime.now(),
        configs={},
        metrics=None,
        files=None,
        add_tags={},
        remove_tags={},
    )

    # when
    result = list(builder)

    # then
    assert len(result) == 1
    operation = result[0]
    expected_update = UpdateRunSnapshot(timestamp=Timestamp(seconds=1722341532, nanos=21934))
    assert operation == expected_update


@freeze_time("2024-07-30 12:12:12.000022")
def test_configs():
    # given
    builder = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        timestamp=datetime.now(),
        configs={
            "some/string": "value",
            "some/int": 2501,
            "some/float": 3.14,
            "some/bool": True,
            "some/datetime": datetime.now(),
            "some/tags": {"tag1", "tag2"},
        },
        metrics=None,
        files=None,
        add_tags={},
        remove_tags={},
    )

    # when
    result = list(builder)

    # then
    assert len(result) == 1
    operation = result[0]
    expected_update = UpdateRunSnapshot(
        timestamp=Timestamp(seconds=1722341532, nanos=21934),
        assign={
            "some/string": Value(string="value"),
            "some/int": Value(int64=2501),
            "some/float": Value(float64=3.14),
            "some/bool": Value(bool=True),
            "some/datetime": Value(timestamp=Timestamp(seconds=1722341532, nanos=21934)),
            "some/tags": Value(string_set=StringSet(values={"tag1", "tag2"})),
        },
    )
    assert operation == expected_update


@freeze_time("2024-07-30 12:12:12.000022")
@pytest.mark.parametrize(
    "preview,preview_completion,expected_preview_proto",
    [
        pytest.param(
            False,
            None,
            None,
            id="no preview",
        ),
        pytest.param(
            False,
            0.5,
            None,
            id="no preview, preview_completion ignored",
        ),
        pytest.param(
            True,
            None,
            Preview(is_preview=True),
            id="preview with no explicit completion",
        ),
        pytest.param(
            True,
            0.5,
            Preview(is_preview=True, completion_ratio=0.5),
            id="preview with specified completion",
        ),
    ],
)
def test_metrics(preview, preview_completion, expected_preview_proto):
    # given
    metrics = Metrics(
        step=1,
        data={
            "some/metric": 3.14,
        },
        preview=preview,
    )
    if preview_completion is not None:
        metrics.preview_completion = preview_completion

    # and

    builder = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        timestamp=datetime.now(),
        configs={},
        metrics=metrics,
        files=None,
        add_tags={},
        remove_tags={},
    )

    # when
    result = list(builder)

    # then
    assert len(result) == 1
    operation = result[0]
    expected_update = UpdateRunSnapshot(
        step=Step(whole=1, micro=0),
        timestamp=Timestamp(seconds=1722341532, nanos=21934),
        preview=expected_preview_proto,
        append={
            "some/metric": Value(float64=3.14),
        },
    )
    assert operation == expected_update


@freeze_time("2024-07-30 12:12:12.000022")
def test_tags():
    # given
    builder = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        timestamp=datetime.now(),
        configs={},
        metrics=None,
        files=None,
        add_tags={
            "some/tags": {"tag1", "tag2"},
            "some/other_tags2": {"tag2", "tag3"},
        },
        remove_tags={
            "some/group_tags": {"tag0", "tag1"},
            "some/other_tags": {"tag2", "tag3"},
        },
    )

    # when
    result = list(builder)

    # then
    assert len(result) == 1
    operation = result[0]
    expected_update = UpdateRunSnapshot(
        timestamp=Timestamp(seconds=1722341532, nanos=21934),
        modify_sets={
            "some/tags": ModifySet(
                string=ModifyStringSet(values={"tag1": SET_OPERATION.ADD, "tag2": SET_OPERATION.ADD})
            ),
            "some/other_tags2": ModifySet(
                string=ModifyStringSet(values={"tag2": SET_OPERATION.ADD, "tag3": SET_OPERATION.ADD})
            ),
            "some/group_tags": ModifySet(
                string=ModifyStringSet(values={"tag0": SET_OPERATION.REMOVE, "tag1": SET_OPERATION.REMOVE})
            ),
            "some/other_tags": ModifySet(
                string=ModifyStringSet(values={"tag2": SET_OPERATION.REMOVE, "tag3": SET_OPERATION.REMOVE})
            ),
        },
    )
    assert operation == expected_update


def test_files():
    files = {f"file{i}": FileRefData(f"dest{i}", mime_type=f"mime{i}", size_bytes=i) for i in range(1000)}

    builder = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        timestamp=datetime.now(),
        configs={},
        metrics=None,
        files=files,
        add_tags={},
        remove_tags={},
        max_message_bytes_size=512,
    )

    result = list(builder)
    assert len(result) > 1

    # Gather all generated FileRef assigns and check if output matches input
    all_assigns = {}
    for op in result:
        all_assigns.update(op.assign)

    assert len(all_assigns) == len(files)
    assert sorted(all_assigns.keys()) == sorted(files.keys())
    for key, value in all_assigns.items():
        file_ref = value.file_ref
        file = files[key]

        assert file_ref.path == file.destination
        assert file_ref.mime_type == file.mime_type
        assert file_ref.size_bytes == file.size_bytes


@freeze_time("2024-07-30 12:12:12.000022")
@patch("neptune_scale.sync.metadata_splitter.INVALID_VALUE_ACTION", "raise")
def test_splitting():
    # given
    max_size = 1024
    timestamp = datetime.now()
    configs = {f"config{v}": v for v in range(1000)}
    add_tags = {f"add/tag{v}": {f"value{v}"} for v in range(1000)}
    remove_tags = {f"remove/tag{v}": {f"value{v}"} for v in range(1000)}
    metrics = Metrics(
        step=1,
        data={f"metric{v}": 7 / 9.0 * v for v in range(1000)},
        preview=True,
    )
    files = {f"file{v}": FileRefData(destination=f"file{v}", mime_type="text/plain", size_bytes=100) for v in range(25)}

    # and
    builder = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        timestamp=timestamp,
        configs=configs,
        metrics=metrics,
        files=files,
        add_tags=add_tags,
        remove_tags=remove_tags,
        max_message_bytes_size=max_size,
    )

    # when
    result = list(builder)

    # then
    assert len(result) > 1

    # Every message should be smaller than max_size
    assert all(len(op.SerializeToString()) <= max_size for op in result)

    # Common metadata
    assert all(op.step.whole == 1 for op in result)
    assert all(op.preview.is_preview if len(op.append) > 0 else True for op in result)
    assert all(op.timestamp == Timestamp(seconds=1722341532, nanos=21934) for op in result)

    # Check if all metrics, configs and tags are present in the result
    assert sorted([key for op in result for key in op.append.keys()]) == sorted(list(metrics.data.keys()))
    assert sorted([key for op in result for key in op.assign.keys() if key.startswith("config")]) == sorted(
        list(configs.keys())
    )
    assert sorted([key for op in result for key in op.assign.keys() if key.startswith("file")]) == sorted(
        list(files.keys())
    )
    assert sorted([key for op in result for key in op.modify_sets.keys()]) == sorted(
        list(add_tags.keys()) + list(remove_tags.keys())
    )


@freeze_time("2024-07-30 12:12:12.000022")
def test_split_large_tags():
    # given
    max_size = 1024
    timestamp = datetime.now()
    metrics = None
    fields = {}
    add_tags = {"add/tag": {f"value{v}" for v in range(1000)}}
    remove_tags = {"remove/tag": {f"value{v}" for v in range(1000)}}

    # and
    builder = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        timestamp=timestamp,
        configs=fields,
        metrics=metrics,
        files=None,
        add_tags=add_tags,
        remove_tags=remove_tags,
        max_message_bytes_size=max_size,
    )

    # when
    result = list(builder)

    # then
    assert len(result) > 1

    # Every message should be smaller than max_size
    assert all(len(op.SerializeToString()) <= max_size for op in result)

    # Common metadata
    assert all(op.timestamp == Timestamp(seconds=1722341532, nanos=21934) for op in result)

    # Check if all StringSet values are split correctly
    assert {key for op in result for key in op.modify_sets.keys()} == set(
        list(add_tags.keys()) + list(remove_tags.keys())
    )

    # Check if all tags are present in the result
    assert {tag for op in result for tag in op.modify_sets["add/tag"].string.values.keys()} == add_tags["add/tag"]
    assert {tag for op in result for tag in op.modify_sets["remove/tag"].string.values.keys()} == remove_tags[
        "remove/tag"
    ]


@patch("neptune_scale.sync.metadata_splitter.SHOULD_SKIP_NON_FINITE_METRICS", False)
@mark.parametrize("value", [np.inf, -np.inf, np.nan, math.inf, -math.inf, math.nan])
def test_raise_on_non_finite_float_metrics(value):
    # given
    metrics = Metrics(
        step=10,
        data={"bad-metric": value},
    )
    splitter = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        timestamp=datetime.now(),
        configs={},
        metrics=metrics,
        files=None,
        add_tags={},
        remove_tags={},
        max_message_bytes_size=1024,
    )

    # when
    with pytest.raises(NeptuneFloatValueNanInfUnsupported) as exc:
        next(splitter)

    # then
    exc.match("metric `bad-metric`")
    exc.match("step `10`")
    exc.match(f"non-finite value of `{value}`")


@mark.parametrize("value", [np.inf, -np.inf, np.nan, math.inf, -math.inf, math.nan])
def test_skip_non_finite_float_metrics(value, caplog):
    with caplog.at_level("WARNING"):
        # given
        metrics = Metrics(
            step=10,
            data={"bad-metric": value},
        )

        # when
        builder = MetadataSplitter(
            project="workspace/project",
            run_id="run_id",
            timestamp=datetime.now(),
            configs={},
            metrics=metrics,
            files=None,
            add_tags={},
            remove_tags={},
            max_message_bytes_size=1024,
        )

        result = list(builder)

        # then
        assert len(result) == 1
        operation = result[0]
        assert not operation.assign

        assert "Skipping a non-finite value" in caplog.text
        assert "bad-metric" in caplog.text


@pytest.mark.parametrize("action", ("raise", "drop"))
@pytest.mark.parametrize("invalid_path", (None, object(), 1, 1.0, True, frozenset(), tuple(), datetime.now()))
@pytest.mark.parametrize("param_name", ("add_tags", "remove_tags", "configs", "metrics", "files"))
def test_invalid_path_types(caplog, action, invalid_path, param_name):
    data = {invalid_path: object()}
    kwargs = {name: None for name in ("add_tags", "remove_tags", "configs", "metrics", "files")}

    if param_name == "metrics":
        data = Metrics(step=1, data=data)
    elif param_name == "files":
        data = {invalid_path: FileRefData(destination="x", mime_type="text/plain", size_bytes=0)}

    kwargs[param_name] = data

    splitter = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        timestamp=datetime.now(),
        **kwargs,
    )

    with patch("neptune_scale.sync.metadata_splitter.INVALID_VALUE_ACTION", action):
        if action == "raise":
            with pytest.raises(NeptuneUnableToLogData, match="paths must be"):
                next(splitter)
        else:
            with caplog.at_level("WARNING"):
                next(splitter)
            assert "paths must be" in caplog.text


@pytest.mark.parametrize("action", ("raise", "drop"))
@pytest.mark.parametrize("invalid_value", (None, "abc", {"nested": 1}, object(), [], set, tuple(), datetime.now()))
def test_invalid_metrics_values(caplog, action, invalid_value):
    # Always have one valid value under the key "ok-value" so we can check that the
    # "drop" action does not drop valid values.
    metrics = {"bad": invalid_value, "ok": 42}

    splitter = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        timestamp=datetime.now(),
        configs=None,
        metrics=Metrics(step=1, data=metrics),
        files=None,
        add_tags={},
        remove_tags={},
    )

    with patch("neptune_scale.sync.metadata_splitter.INVALID_VALUE_ACTION", action):
        if action == "raise":
            with pytest.raises(NeptuneUnableToLogData, match="values must be"):
                next(splitter)
        else:
            with caplog.at_level("WARNING"):
                result = list(splitter)

            assert len(result[0].append) == 1
            assert "ok" in result[0].append
            assert "values must be" in caplog.text


@pytest.mark.parametrize("action", ("raise", "drop"))
@pytest.mark.parametrize("invalid_value", (None, {"nested": 1}, object()))
def test_invalid_configs_values(caplog, action, invalid_value):
    # Always have one valid value under the key "ok-value" so we can check that the
    # "drop" action does not drop valid values.
    configs = {"bad": invalid_value, "ok": 42}
    splitter = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        timestamp=datetime.now(),
        configs=configs,
        metrics=None,
        files=None,
        add_tags={},
        remove_tags={},
    )

    with patch("neptune_scale.sync.metadata_splitter.INVALID_VALUE_ACTION", action):
        if action == "raise":
            with pytest.raises(NeptuneUnableToLogData, match="values must be"):
                next(splitter)
        else:
            with caplog.at_level("WARNING"):
                result = list(splitter)

            assert len(result[0].assign) == 1
            assert "ok" in result[0].assign
            assert "values must be" in caplog.text


@pytest.mark.parametrize("action", ("raise", "drop"))
@pytest.mark.parametrize("operation", ("add", "remove"))
@pytest.mark.parametrize("invalid_value", (None, {"nested": 1}, object(), "abc", 1, 1.0, True, datetime.now()))
def test_invalid_tags_values(caplog, action, operation, invalid_value):
    # Always have one valid value under the key "ok-value" so we can check that the
    # "drop" action does not drop valid values.
    tags = {"bad": invalid_value, "ok": ["tag"]}
    splitter = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        timestamp=datetime.now(),
        configs=None,
        metrics=None,
        files=None,
        add_tags=tags if operation == "add" else {},
        remove_tags=tags if operation == "remove" else {},
    )

    with patch("neptune_scale.sync.metadata_splitter.INVALID_VALUE_ACTION", action):
        if action == "raise":
            with pytest.raises(NeptuneUnableToLogData, match="Tags must be a"):
                next(splitter)
        else:
            with caplog.at_level("WARNING"):
                result = list(splitter)

            assert len(result[0].modify_sets) == 1
            assert "ok" in result[0].modify_sets
            assert "Tags must be a" in caplog.text


@pytest.mark.parametrize("action", ("raise", "drop"))
@pytest.mark.parametrize(
    "invalid_value",
    (
        FileRefData(destination="D" * 1024, mime_type="M", size_bytes=0),
        FileRefData(destination="D", mime_type="M" * 1024, size_bytes=0),
    ),
)
def test_too_long_files_values(caplog, action, invalid_value):
    # Always have one valid value under the key "ok- so we can check that the
    # "drop" action does not drop valid values.
    files = {"bad": invalid_value, "ok": FileRefData("destination", "mime_type", 0)}
    splitter = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        timestamp=datetime.now(),
        configs=None,
        metrics=None,
        files=files,
        add_tags={},
        remove_tags={},
    )

    with patch("neptune_scale.sync.metadata_splitter.INVALID_VALUE_ACTION", action):
        if action == "raise":
            with pytest.raises(NeptuneUnableToLogData, match="must be a string of at most"):
                next(splitter)
        else:
            with caplog.at_level("WARNING"):
                result = list(splitter)

            assert len(result[0].assign) == 1
            assert "ok" in result[0].assign
            assert "must be a string of at most" in caplog.text
