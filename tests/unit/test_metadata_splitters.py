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

from neptune_scale.core.metadata_splitters import (
    EstimationBased,
    NoSplitting,
    SerializationBased,
)


class TestNoSplitting:
    @freeze_time("2024-07-30 12:12:12.000022")
    def test_empty(self):
        # given
        builder = NoSplitting(
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
    def test_fields(self):
        # given
        builder = NoSplitting(
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
    def test_metrics(self):
        # given
        builder = NoSplitting(
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
    def test_tags(self):
        # given
        builder = NoSplitting(
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


class TestSerializationBased:
    @freeze_time("2024-07-30 12:12:12.000022")
    def test_empty(self):
        # given
        builder = SerializationBased(
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
    def test_fields(self):
        # given
        builder = SerializationBased(
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
    def test_metrics(self):
        # given
        builder = SerializationBased(
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
    def test_tags(self):
        # given
        builder = SerializationBased(
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

    @freeze_time("2024-07-30 12:12:12.000022")
    def test_splitting(self):
        # given
        max_size = 1024
        timestamp = datetime.now()
        metrics = {f"metric{v}": 7 / 9.0 * v for v in range(1000)}
        fields = {f"field{v}": v for v in range(1000)}
        add_tags = {f"add/tag{v}": {f"value{v}"} for v in range(1000)}
        remove_tags = {f"remove/tag{v}": {f"value{v}"} for v in range(1000)}

        # and
        builder = SerializationBased(
            project="workspace/project",
            run_id="run_id",
            step=1,
            timestamp=timestamp,
            fields=fields,
            metrics=metrics,
            add_tags=add_tags,
            remove_tags=remove_tags,
            max_message_bytes_size=max_size,
        )

        # when
        result = list(builder)

        # then
        assert len(result) > 0
        assert all(len(op.SerializeToString()) <= max_size for op in result)
        assert all(op.project == "workspace/project" for op in result)
        assert all(op.run_id == "run_id" for op in result)
        assert all(op.update.step.whole == 1 for op in result)
        assert all(op.update.timestamp == Timestamp(seconds=1722341532, nanos=21934) for op in result)
        assert sorted([key for op in result for key in op.update.append.keys()]) == sorted(list(metrics.keys()))
        assert sorted([key for op in result for key in op.update.assign.keys()]) == sorted(list(fields.keys()))
        assert sorted([key for op in result for key in op.update.modify_sets.keys()]) == sorted(
            list(add_tags.keys()) + list(remove_tags.keys())
        )


class TestEstimationBased:
    @freeze_time("2024-07-30 12:12:12.000022")
    def test_empty(self):
        # given
        builder = EstimationBased(
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
    def test_fields(self):
        # given
        builder = EstimationBased(
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
    def test_metrics(self):
        # given
        builder = EstimationBased(
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
    def test_tags(self):
        # given
        builder = EstimationBased(
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

    @freeze_time("2024-07-30 12:12:12.000022")
    def test_splitting(self):
        # given
        max_size = 1024
        timestamp = datetime.now()
        metrics = {f"metric{v}": 7 / 9.0 * v for v in range(1000)}
        fields = {f"field{v}": v for v in range(1000)}
        add_tags = {f"add/tag{v}": {f"value{v}"} for v in range(1000)}
        remove_tags = {f"remove/tag{v}": {f"value{v}"} for v in range(1000)}

        # and
        builder = EstimationBased(
            project="workspace/project",
            run_id="run_id",
            step=1,
            timestamp=timestamp,
            fields=fields,
            metrics=metrics,
            add_tags=add_tags,
            remove_tags=remove_tags,
            max_message_bytes_size=max_size,
        )

        # when
        result = list(builder)

        # then
        assert len(result) > 0

        # Every message should be smaller than max_size
        assert all(len(op.SerializeToString()) <= max_size for op in result)

        # Common metadata
        assert all(op.project == "workspace/project" for op in result)
        assert all(op.run_id == "run_id" for op in result)
        assert all(op.update.step.whole == 1 for op in result)
        assert all(op.update.timestamp == Timestamp(seconds=1722341532, nanos=21934) for op in result)

        # Check if all metrics, fields and tags are present in the result
        assert sorted([key for op in result for key in op.update.append.keys()]) == sorted(list(metrics.keys()))
        assert sorted([key for op in result for key in op.update.assign.keys()]) == sorted(list(fields.keys()))
        assert sorted([key for op in result for key in op.update.modify_sets.keys()]) == sorted(
            list(add_tags.keys()) + list(remove_tags.keys())
        )

    @freeze_time("2024-07-30 12:12:12.000022")
    def test_split_large_tags(self):
        # given
        max_size = 1024
        timestamp = datetime.now()
        metrics = {}
        fields = {}
        add_tags = {"add/tag": {f"value{v}" for v in range(1000)}}
        remove_tags = {"remove/tag": {f"value{v}" for v in range(1000)}}

        # and
        builder = EstimationBased(
            project="workspace/project",
            run_id="run_id",
            step=1,
            timestamp=timestamp,
            fields=fields,
            metrics=metrics,
            add_tags=add_tags,
            remove_tags=remove_tags,
            max_message_bytes_size=max_size,
        )

        # when
        result = list(builder)

        # then
        assert len(result) > 0

        # Every message should be smaller than max_size
        assert all(len(op.SerializeToString()) <= max_size for op in result)

        # Common metadata
        assert all(op.project == "workspace/project" for op in result)
        assert all(op.run_id == "run_id" for op in result)
        assert all(op.update.step.whole == 1 for op in result)
        assert all(op.update.timestamp == Timestamp(seconds=1722341532, nanos=21934) for op in result)

        # Check if all StringSet values are split correctly
        assert set([key for op in result for key in op.update.modify_sets.keys()]) == set(
            list(add_tags.keys()) + list(remove_tags.keys())
        )

        # Check if all tags are present in the result
        assert {tag for op in result for tag in op.update.modify_sets["add/tag"].string.values.keys()} == add_tags[
            "add/tag"
        ]
        assert {
            tag for op in result for tag in op.update.modify_sets["remove/tag"].string.values.keys()
        } == remove_tags["remove/tag"]
