from datetime import datetime

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

from neptune_scale.core.message_builder import MessageBuilder


@freeze_time("2024-07-30 12:12:12.000022")
def test_empty():
    # given
    builder = MessageBuilder(
        project="workspace/project",
        run_id="run_id",
        step=1,
        timestamp=datetime.now(),
        fields={},
        metrics={},
        add_tags={},
        remove_tags={},
    )

    # when
    result = list(builder)

    # then
    assert len(result) == 1
    assert result[0] == RunOperation(
        project="workspace/project",
        run_id="run_id",
        update=UpdateRunSnapshot(step=Step(whole=1, micro=0), timestamp=Timestamp(seconds=1722341532, nanos=21934)),
    )


@freeze_time("2024-07-30 12:12:12.000022")
def test_fields():
    # given
    builder = MessageBuilder(
        project="workspace/project",
        run_id="run_id",
        step=1,
        timestamp=datetime.now(),
        fields={
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
    assert result[0] == RunOperation(
        project="workspace/project",
        run_id="run_id",
        update=UpdateRunSnapshot(
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
        ),
    )


@freeze_time("2024-07-30 12:12:12.000022")
def test_metrics():
    # given
    builder = MessageBuilder(
        project="workspace/project",
        run_id="run_id",
        step=1,
        timestamp=datetime.now(),
        fields={},
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
    assert result[0] == RunOperation(
        project="workspace/project",
        run_id="run_id",
        update=UpdateRunSnapshot(
            step=Step(whole=1, micro=0),
            timestamp=Timestamp(seconds=1722341532, nanos=21934),
            append={
                "some/metric": Value(float64=3.14),
            },
        ),
    )


@freeze_time("2024-07-30 12:12:12.000022")
def test_tags():
    # given
    builder = MessageBuilder(
        project="workspace/project",
        run_id="run_id",
        step=1,
        timestamp=datetime.now(),
        fields={},
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
    assert result[0] == RunOperation(
        project="workspace/project",
        run_id="run_id",
        update=UpdateRunSnapshot(
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
        ),
    )
