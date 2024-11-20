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
    Step,
    StringSet,
    UpdateRunSnapshot,
    Value,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation
from pytest import mark

from neptune_scale.core.metadata_splitter import MetadataSplitter
from neptune_scale.exceptions import NeptuneFloatValueNanInfUnsupported


@freeze_time("2024-07-30 12:12:12.000022")
def test_empty():
    # given
    builder = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        step=1,
        timestamp=datetime.now(),
        configs={},
        metrics={},
        add_tags={},
        remove_tags={},
    )

    # when
    result = list(builder)

    # then
    assert len(result) == 1
    operation, metadata_size = result[0]
    expected_update = UpdateRunSnapshot(
        step=Step(whole=1, micro=0), timestamp=Timestamp(seconds=1722341532, nanos=21934)
    )
    assert operation == RunOperation(project="workspace/project", run_id="run_id", update=expected_update)
    assert metadata_size == expected_update.ByteSize()


@freeze_time("2024-07-30 12:12:12.000022")
def test_fields():
    # given
    builder = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        step=1,
        timestamp=datetime.now(),
        configs={
            "some/string": "value",
            "some/int": 2501,
            "some/float": 3.14,
            "some/bool": True,
            "some/datetime": datetime.now(),
            "some/tags": {"tag1", "tag2"},
        },
        metrics={},
        add_tags={},
        remove_tags={},
    )

    # when
    result = list(builder)

    # then
    assert len(result) == 1
    operation, metadata_size = result[0]
    expected_update = UpdateRunSnapshot(
        step=Step(whole=1, micro=0),
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
    assert operation == RunOperation(project="workspace/project", run_id="run_id", update=expected_update)
    assert metadata_size >= expected_update.ByteSize()
    assert metadata_size < operation.ByteSize()


@freeze_time("2024-07-30 12:12:12.000022")
def test_metrics():
    # given
    builder = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        step=1,
        timestamp=datetime.now(),
        configs={},
        metrics={
            "some/metric": 3.14,
        },
        add_tags={},
        remove_tags={},
    )

    # when
    result = list(builder)

    # then
    assert len(result) == 1
    operation, metadata_size = result[0]
    expected_update = UpdateRunSnapshot(
        step=Step(whole=1, micro=0),
        timestamp=Timestamp(seconds=1722341532, nanos=21934),
        append={
            "some/metric": Value(float64=3.14),
        },
    )
    assert operation == RunOperation(project="workspace/project", run_id="run_id", update=expected_update)
    assert metadata_size >= expected_update.ByteSize()
    assert metadata_size < operation.ByteSize()


@freeze_time("2024-07-30 12:12:12.000022")
def test_tags():
    # given
    builder = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        step=1,
        timestamp=datetime.now(),
        configs={},
        metrics={},
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
    operation, metadata_size = result[0]
    expected_update = UpdateRunSnapshot(
        step=Step(whole=1, micro=0),
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
    assert operation == RunOperation(project="workspace/project", run_id="run_id", update=expected_update)
    assert metadata_size >= expected_update.ByteSize()
    assert metadata_size < operation.ByteSize()


@freeze_time("2024-07-30 12:12:12.000022")
def test_splitting():
    # given
    max_size = 1024
    timestamp = datetime.now()
    metrics = {f"metric{v}": 7 / 9.0 * v for v in range(1000)}
    fields = {f"field{v}": v for v in range(1000)}
    add_tags = {f"add/tag{v}": {f"value{v}"} for v in range(1000)}
    remove_tags = {f"remove/tag{v}": {f"value{v}"} for v in range(1000)}

    # and
    builder = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        step=1,
        timestamp=timestamp,
        configs=fields,
        metrics=metrics,
        add_tags=add_tags,
        remove_tags=remove_tags,
        max_message_bytes_size=max_size,
    )

    # when
    result = list(builder)

    # then
    assert len(result) > 1

    # Every message should be smaller than max_size
    assert all(len(op.SerializeToString()) <= max_size for op, _ in result)

    # Common metadata
    assert all(op.project == "workspace/project" for op, _ in result)
    assert all(op.run_id == "run_id" for op, _ in result)
    assert all(op.update.step.whole == 1 for op, _ in result)
    assert all(op.update.timestamp == Timestamp(seconds=1722341532, nanos=21934) for op, _ in result)

    # Check if all metrics, fields and tags are present in the result
    assert sorted([key for op, _ in result for key in op.update.append.keys()]) == sorted(list(metrics.keys()))
    assert sorted([key for op, _ in result for key in op.update.assign.keys()]) == sorted(list(fields.keys()))
    assert sorted([key for op, _ in result for key in op.update.modify_sets.keys()]) == sorted(
        list(add_tags.keys()) + list(remove_tags.keys())
    )


@freeze_time("2024-07-30 12:12:12.000022")
def test_split_large_tags():
    # given
    max_size = 1024
    timestamp = datetime.now()
    metrics = {}
    fields = {}
    add_tags = {"add/tag": {f"value{v}" for v in range(1000)}}
    remove_tags = {"remove/tag": {f"value{v}" for v in range(1000)}}

    # and
    builder = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        step=1,
        timestamp=timestamp,
        configs=fields,
        metrics=metrics,
        add_tags=add_tags,
        remove_tags=remove_tags,
        max_message_bytes_size=max_size,
    )

    # when
    result = list(builder)

    # then
    assert len(result) > 1

    # Every message should be smaller than max_size
    assert all(len(op.SerializeToString()) <= max_size for op, _ in result)

    # Common metadata
    assert all(op.project == "workspace/project" for op, _ in result)
    assert all(op.run_id == "run_id" for op, _ in result)
    assert all(op.update.step.whole == 1 for op, _ in result)
    assert all(op.update.timestamp == Timestamp(seconds=1722341532, nanos=21934) for op, _ in result)

    # Check if all StringSet values are split correctly
    assert set([key for op, _ in result for key in op.update.modify_sets.keys()]) == set(
        list(add_tags.keys()) + list(remove_tags.keys())
    )

    # Check if all tags are present in the result
    assert {tag for op, _ in result for tag in op.update.modify_sets["add/tag"].string.values.keys()} == add_tags[
        "add/tag"
    ]
    assert {tag for op, _ in result for tag in op.update.modify_sets["remove/tag"].string.values.keys()} == remove_tags[
        "remove/tag"
    ]


@patch("neptune_scale.core.metadata_splitter.SKIP_NON_FINITE_METRICS", False)
@mark.parametrize("value", [np.inf, -np.inf, np.nan, math.inf, -math.inf, math.nan])
def test_raise_on_non_finite_float_metrics(value):
    builder = MetadataSplitter(
        project="workspace/project",
        run_id="run_id",
        step=10,
        timestamp=datetime.now(),
        configs={},
        metrics={"metric": value},
        add_tags={},
        remove_tags={},
        max_message_bytes_size=1024,
    )

    with pytest.raises(NeptuneFloatValueNanInfUnsupported) as exc:
        list(builder)

    exc.match(f"step.*10.*value.*{value}")


@mark.parametrize("value", [np.inf, -np.inf, np.nan, math.inf, -math.inf, math.nan])
def test_skip_non_finite_float_metrics(value, caplog):
    with caplog.at_level("WARNING"):
        builder = MetadataSplitter(
            project="workspace/project",
            run_id="run_id",
            step=10,
            timestamp=datetime.now(),
            configs={},
            metrics={"bad-metric": value},
            add_tags={},
            remove_tags={},
            max_message_bytes_size=1024,
        )

        result = list(builder)
        assert len(result) == 1

        op, _ = result[0]
        assert not op.update.assign

        assert "Skipping a non-finite value" in caplog.text
        assert "bad-metric" in caplog.text
