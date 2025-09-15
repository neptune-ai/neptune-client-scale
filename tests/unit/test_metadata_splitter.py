from datetime import datetime
from unittest.mock import patch

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

from neptune_scale.exceptions import NeptuneUnableToLogData
from neptune_scale.sync.metadata_splitter import (
    FileRefData,
    MetadataSplitter,
    Metrics,
    decompose_step,
    string_series_to_update_run_snapshots,
)

# The character "𐍈" (U+10348) encodes to 4 bytes
UTF_CHAR = "𐍈"


@pytest.mark.parametrize(
    "step, expect_whole, expect_micro",
    (
        (0, 0, 0),
        (100_000_000.999999, 100_000_000, 999999),
        (1.0000001, 1, 0),
        (1.000001, 1, 1),
        (0.123456, 0, 123456),
        (0.9999999999, 0, 999999),
        (0.000001, 0, 1),
        (0.000012, 0, 12),
        (0.000123, 0, 123),
        (0.001234, 0, 1234),
        (0.012345, 0, 12345),
        (0.123456, 0, 123456),
        (1.1, 1, 100000),
        (1.12, 1, 120000),
        (1.123, 1, 123000),
        (1.1234, 1, 123400),
        (1.12345, 1, 123450),
        (1.123456, 1, 123456),
    ),
)
def test_decompose_step(step, expect_whole, expect_micro):
    whole, micro = decompose_step(step)
    assert whole == expect_whole
    assert micro == expect_micro


@freeze_time("2024-07-30 12:12:12.000022")
def test_empty():
    # given
    builder = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        timestamp=datetime.now(),
        step=None,
        configs={},
        metrics=None,
        files=None,
        file_series=None,
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
        step=None,
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
        file_series=None,
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
        step=1,
        configs={},
        metrics=metrics,
        files=None,
        file_series=None,
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
        step=None,
        configs={},
        metrics=None,
        files=None,
        file_series=None,
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
        step=None,
        configs={},
        metrics=None,
        files=files,
        file_series=None,
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


def test_file_series():
    step = 3
    file_series = {f"file{i}": FileRefData(f"dest{i}", mime_type=f"mime{i}", size_bytes=i) for i in range(1000)}

    builder = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        timestamp=datetime.now(),
        step=step,
        configs={},
        metrics=None,
        files=None,
        file_series=file_series,
        add_tags={},
        remove_tags={},
        max_message_bytes_size=512,
    )

    result = list(builder)
    assert len(result) > 1

    # Gather all generated FileRef assigns and check if output matches input
    all_appends = {}
    for op in result:
        all_appends.update(op.append)

    assert len(all_appends) == len(file_series)
    assert sorted(all_appends.keys()) == sorted(file_series.keys())
    for key, value in all_appends.items():
        file_ref = value.file_ref
        file = file_series[key]

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
        data={f"metric{v}": 7 / 9.0 * v for v in range(1000)},
        preview=True,
    )
    files = {
        f"file_atom{v}": FileRefData(destination=f"file{v}", mime_type="text/plain", size_bytes=100) for v in range(25)
    }
    file_series = {
        f"file_series{v}": FileRefData(destination=f"file_series{v}", mime_type="text/plain", size_bytes=100)
        for v in range(25)
    }

    # and
    builder = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        timestamp=timestamp,
        step=1,
        configs=configs,
        metrics=metrics,
        files=files,
        file_series=file_series,
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
    assert sorted([key for op in result for key in op.append.keys() if key.startswith("metric")]) == sorted(
        list(metrics.data.keys())
    )
    assert sorted([key for op in result for key in op.append.keys() if key.startswith("file_series")]) == sorted(
        list(file_series.keys())
    )
    assert sorted([key for op in result for key in op.assign.keys() if key.startswith("config")]) == sorted(
        list(configs.keys())
    )
    assert sorted([key for op in result for key in op.assign.keys() if key.startswith("file_atom")]) == sorted(
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
        step=None,
        configs=fields,
        metrics=metrics,
        files=None,
        file_series=None,
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


@pytest.mark.parametrize("action", ("raise", "drop"))
@pytest.mark.parametrize(
    "invalid_path",
    (None, "A" * 1025, UTF_CHAR * 256 + "A", object(), 1, 1.0, True, frozenset(), tuple(), datetime.now()),
)
@pytest.mark.parametrize("param_name", ("add_tags", "remove_tags", "configs", "metrics", "files", "file_series"))
def test_invalid_paths(caplog, action, invalid_path, param_name):
    data = {invalid_path: object()}
    step = None
    kwargs = {name: None for name in ("add_tags", "remove_tags", "configs", "metrics", "files", "file_series")}

    if param_name == "metrics":
        data = Metrics(data=data)
        step = 1
    elif param_name == "files":
        data = {invalid_path: FileRefData(destination="x", mime_type="text/plain", size_bytes=0)}
    elif param_name == "file_series":
        data = {invalid_path: FileRefData(destination="x", mime_type="text/plain", size_bytes=0)}
        step = 1

    kwargs[param_name] = data

    splitter = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        timestamp=datetime.now(),
        step=step,
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


@pytest.mark.parametrize("path", ("A", "A" * 1024, UTF_CHAR * 256))
@patch("neptune_scale.sync.metadata_splitter.INVALID_VALUE_ACTION", "raise")
@pytest.mark.parametrize(
    "param_and_value",
    (
        ("add_tags", ["tag"]),
        ("remove_tags", ["tag"]),
        ("configs", "foo"),
        ("metrics", None),
        ("files", None),
        ("file_series", None),
    ),
)
def test_valid_paths(caplog, path, param_and_value):
    param_name, value = param_and_value
    step = None
    kwargs = {name: None for name in ("add_tags", "remove_tags", "configs", "metrics", "files", "file_series")}

    if param_name == "metrics":
        data = Metrics(data={path: 1})
        step = 1
    elif param_name == "files":
        data = {path: FileRefData(destination="x", mime_type="text/plain", size_bytes=0)}
    elif param_name == "file_series":
        data = {path: FileRefData(destination="x", mime_type="text/plain", size_bytes=0)}
        step = 1
    else:
        data = {path: value}

    kwargs[param_name] = data

    splitter = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        timestamp=datetime.now(),
        step=step,
        **kwargs,
    )

    # Shouldn't fail
    next(splitter)


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
        step=1,
        configs=None,
        metrics=Metrics(data=metrics),
        files=None,
        file_series=None,
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
        step=None,
        configs=configs,
        metrics=None,
        files=None,
        file_series=None,
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
        step=None,
        configs=None,
        metrics=None,
        files=None,
        file_series=None,
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
@pytest.mark.parametrize("parameter", ("files", "file_series"))
def test_too_long_files_values(caplog, action, invalid_value, parameter):
    # Always have one valid value under the key "ok- so we can check that the
    # "drop" action does not drop valid values.
    files, file_series, step = None, None, None
    value = {"bad": invalid_value, "ok": FileRefData("destination", "mime_type", 0)}
    if parameter == "files":
        files = value
    if parameter == "file_series":
        step = 1
        file_series = value

    splitter = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        timestamp=datetime.now(),
        step=step,
        configs=None,
        metrics=None,
        files=files,
        file_series=file_series,
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

            if parameter == "files":
                assert len(result[0].assign) == 1
                assert "ok" in result[0].assign
            else:
                assert len(result[0].append) == 1
                assert "ok" in result[0].append
            assert "must be a string of at most" in caplog.text


@freeze_time("2024-07-30 12:12:12.000022")
def test_string_series_to_operations():
    max_size = 512
    string_series = {f"string{i}": f"value{i}" for i in range(1000)}
    timestamp = datetime.now()
    updates = string_series_to_update_run_snapshots(string_series, step=1, timestamp=timestamp, max_size=max_size)

    result = list(updates)
    assert len(result) > 1

    assert all(len(op.SerializeToString()) <= max_size for op in result)
    assert all(op.step.whole == 1 for op in result)
    assert all(op.timestamp == Timestamp(seconds=1722341532, nanos=21934) for op in result)

    assert all(not op.HasField("preview") for op in result), "preview should not be present"
    assert all(not op.assign for op in result), "no assigns should be set"
    assert all(not op.modify_sets for op in result), "no modify_sets should be set"

    # Gather all UpdateRunSnapshot.append data and compare against input
    append_values = {key: value.string for op in result for key, value in op.append.items()}
    assert append_values == string_series, "aggregated values are different"


@pytest.mark.parametrize("action", ("raise", "drop"))
@pytest.mark.parametrize(
    "invalid_path",
    (
        None,
        "A" * 1025,
        "A" + UTF_CHAR * 256,
        UTF_CHAR * 257,
        object(),
        1,
        1.0,
        True,
        frozenset(),
        tuple(),
        datetime.now(),
    ),
)
def test_string_series_invalid_paths(caplog, action, invalid_path):
    data: dict[str, str] = {invalid_path: object()}  # type: ignore
    with patch("neptune_scale.sync.metadata_splitter.INVALID_VALUE_ACTION", action):
        if action == "raise":
            with pytest.raises(NeptuneUnableToLogData, match="paths must be"):
                list(string_series_to_update_run_snapshots(data, step=1, timestamp=datetime.now()))
        else:
            with caplog.at_level("WARNING"):
                list(string_series_to_update_run_snapshots(data, step=1, timestamp=datetime.now()))
            assert "paths must be" in caplog.text


@pytest.mark.parametrize("path", ("A", "A" * 1024, UTF_CHAR * 256))
@patch("neptune_scale.sync.metadata_splitter.INVALID_VALUE_ACTION", "raise")
def test_string_series_valid_paths(path):
    data = {path: "foo"}
    list(string_series_to_update_run_snapshots(data, step=1, timestamp=datetime.now()))


@pytest.mark.parametrize("action", ("raise", "drop"))
@pytest.mark.parametrize(
    "invalid_value",
    (1204 * 1024 + 1, None, {"a-dict": 1}, 1, 1.0, object(), [], set, tuple(), datetime.now()),
    # Don't let pytest print large strings in case of failure
    ids=lambda val: f"<{len(val)}-byte string>" if isinstance(val, str) else None,
)
def test_string_series_invalid_values(caplog, action, invalid_value):
    data = {"bad-value": invalid_value, "valid-value": "value-that-must-not-be-dropped"}
    with patch("neptune_scale.sync.metadata_splitter.INVALID_VALUE_ACTION", action):
        if action == "raise":
            with pytest.raises(NeptuneUnableToLogData, match="values must be"):
                list(string_series_to_update_run_snapshots(data, step=1, timestamp=datetime.now()))
        else:
            with caplog.at_level("WARNING"):
                result = list(string_series_to_update_run_snapshots(data, step=1, timestamp=datetime.now()))

            assert len(result[0].append) == 1
            assert "valid-value" in result[0].append
            assert "values must be" in caplog.text
