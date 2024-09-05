import time
from queue import (
    Empty,
    Full,
)

import pytest
from freezegun import freeze_time
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import Run as CreateRun
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
    Step,
    UpdateRunSnapshot,
    Value,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale.core.components.aggregating_queue import AggregatingQueue
from neptune_scale.core.components.queue_element import (
    BatchedOperations,
    SingleOperation,
)


@freeze_time("2024-09-01")
def test__simple():
    # given
    update = UpdateRunSnapshot(assign={f"key_{i}": Value(string=("a" * 2)) for i in range(2)})
    operation = RunOperation(update=update)
    element = SingleOperation(
        sequence_id=1,
        timestamp=time.process_time(),
        operation=operation.SerializeToString(),
        is_metadata_update=True,
        metadata_size=update.ByteSize(),
        operation_key=None,
    )

    # and
    queue = AggregatingQueue(max_queue_size=1)

    # when
    queue.put_nowait(element=element)

    # then
    assert queue.get_nowait() == BatchedOperations(
        sequence_id=1,
        timestamp=element.timestamp,
        operation=element.operation,
    )


@freeze_time("2024-09-01")
def test__max_size_exceeded():
    # given
    operation1 = RunOperation()
    operation2 = RunOperation()
    element1 = SingleOperation(
        sequence_id=1,
        timestamp=time.process_time(),
        operation=operation1.SerializeToString(),
        is_metadata_update=True,
        metadata_size=0,
        operation_key=None,
    )
    element2 = SingleOperation(
        sequence_id=2,
        timestamp=time.process_time(),
        operation=operation2.SerializeToString(),
        is_metadata_update=True,
        metadata_size=0,
        operation_key=None,
    )

    # and
    queue = AggregatingQueue(max_queue_size=1)

    # when
    queue.put_nowait(element=element1)

    # then
    assert True

    # when
    with pytest.raises(Full):
        queue.put_nowait(element=element2)


@freeze_time("2024-09-01")
def test__empty():
    # given
    queue = AggregatingQueue(max_queue_size=1)

    # when
    with pytest.raises(Empty):
        _ = queue.get_nowait()


@freeze_time("2024-09-01")
def test__batch_size_limit():
    # given
    update1 = UpdateRunSnapshot(step=None, assign={f"aa{i}": Value(int64=(i * 97)) for i in range(2)})
    update2 = UpdateRunSnapshot(step=None, assign={f"bb{i}": Value(int64=(i * 25)) for i in range(2)})
    operation1 = RunOperation(update=update1)
    operation2 = RunOperation(update=update2)
    element1 = SingleOperation(
        sequence_id=1,
        timestamp=time.process_time(),
        operation=operation1.SerializeToString(),
        is_metadata_update=True,
        metadata_size=update1.ByteSize(),
        operation_key=None,
    )
    element2 = SingleOperation(
        sequence_id=2,
        timestamp=time.process_time(),
        operation=operation2.SerializeToString(),
        is_metadata_update=True,
        metadata_size=update2.ByteSize(),
        operation_key=None,
    )

    # and
    queue = AggregatingQueue(max_queue_size=2, max_elements_in_batch=1)

    # when
    queue.put_nowait(element=element1)
    queue.put_nowait(element=element2)

    # then
    assert queue.get_nowait() == BatchedOperations(
        sequence_id=1,
        timestamp=element1.timestamp,
        operation=element1.operation,
    )
    assert queue.get_nowait() == BatchedOperations(
        sequence_id=2,
        timestamp=element2.timestamp,
        operation=element2.operation,
    )


@freeze_time("2024-09-01")
def test__batching():
    # given
    update1 = UpdateRunSnapshot(step=None, assign={f"aa{i}": Value(int64=(i * 97)) for i in range(2)})
    update2 = UpdateRunSnapshot(step=None, assign={f"bb{i}": Value(int64=(i * 25)) for i in range(2)})

    # and
    operation1 = RunOperation(update=update1, project="project", run_id="run_id")
    operation2 = RunOperation(update=update2, project="project", run_id="run_id")

    # and
    element1 = SingleOperation(
        sequence_id=1,
        timestamp=time.process_time(),
        operation=operation1.SerializeToString(),
        is_metadata_update=True,
        metadata_size=update1.ByteSize(),
        operation_key=None,
    )
    element2 = SingleOperation(
        sequence_id=2,
        timestamp=time.process_time(),
        operation=operation2.SerializeToString(),
        is_metadata_update=True,
        metadata_size=update2.ByteSize(),
        operation_key=None,
    )

    # and
    queue = AggregatingQueue(max_queue_size=2, max_elements_in_batch=2)

    # and
    queue.put_nowait(element=element1)
    queue.put_nowait(element=element2)

    # when
    result = queue.get_nowait()

    # then
    assert result.sequence_id == 2
    assert result.timestamp == element2.timestamp

    # and
    batch = RunOperation()
    batch.ParseFromString(result.operation)

    assert batch.project == "project"
    assert batch.run_id == "run_id"
    assert all(k in batch.update.assign for k in ["aa0", "aa1", "bb0", "bb1"])


@freeze_time("2024-09-01")
def test__not_merge_two_run_creation():
    # given
    create1 = CreateRun(family="family", run_id="run_id1")
    create2 = CreateRun(family="family", run_id="run_id2")

    # and
    operation1 = RunOperation(create=create1, project="project", run_id="run_id1")
    operation2 = RunOperation(create=create2, project="project", run_id="run_id2")

    # and
    element1 = SingleOperation(
        sequence_id=1,
        timestamp=time.process_time(),
        operation=operation1.SerializeToString(),
        is_metadata_update=False,
        metadata_size=0,
        operation_key=None,
    )
    element2 = SingleOperation(
        sequence_id=2,
        timestamp=time.process_time(),
        operation=operation2.SerializeToString(),
        is_metadata_update=False,
        metadata_size=0,
        operation_key=None,
    )

    # and
    queue = AggregatingQueue(max_queue_size=2, max_elements_in_batch=2)

    # and
    queue.put_nowait(element=element1)
    queue.put_nowait(element=element2)

    # when
    result = queue.get_nowait()

    # then
    assert result.sequence_id == 1
    assert result.timestamp == element1.timestamp

    # and
    batch = RunOperation()
    batch.ParseFromString(result.operation)

    assert batch.project == "project"
    assert batch.run_id == "run_id1"
    assert batch.create == create1

    # when
    result = queue.get_nowait()

    # then
    assert result.sequence_id == 2
    assert result.timestamp == element2.timestamp

    # and
    batch = RunOperation()
    batch.ParseFromString(result.operation)

    assert batch.project == "project"
    assert batch.run_id == "run_id2"
    assert batch.create == create2


@freeze_time("2024-09-01")
def test__not_merge_run_creation_with_metadata_update():
    # given
    create = CreateRun(family="family", run_id="run_id")
    update = UpdateRunSnapshot(step=None, assign={f"bb{i}": Value(int64=(i * 25)) for i in range(2)})

    # and
    operation1 = RunOperation(create=create, project="project", run_id="run_id")
    operation2 = RunOperation(update=update, project="project", run_id="run_id")

    # and
    element1 = SingleOperation(
        sequence_id=1,
        timestamp=time.process_time(),
        operation=operation1.SerializeToString(),
        is_metadata_update=False,
        metadata_size=0,
        operation_key=None,
    )
    element2 = SingleOperation(
        sequence_id=2,
        timestamp=time.process_time(),
        operation=operation2.SerializeToString(),
        is_metadata_update=True,
        metadata_size=update.ByteSize(),
        operation_key=None,
    )

    # and
    queue = AggregatingQueue(max_queue_size=2, max_elements_in_batch=2)

    # and
    queue.put_nowait(element=element1)
    queue.put_nowait(element=element2)

    # when
    result = queue.get_nowait()

    # then
    assert result.sequence_id == 1
    assert result.timestamp == element1.timestamp

    # and
    batch = RunOperation()
    batch.ParseFromString(result.operation)

    assert batch.project == "project"
    assert batch.run_id == "run_id"
    assert batch.create == create

    # when
    result = queue.get_nowait()

    # then
    assert result.sequence_id == 2
    assert result.timestamp == element2.timestamp

    # and
    batch = RunOperation()
    batch.ParseFromString(result.operation)

    assert batch.project == "project"
    assert batch.run_id == "run_id"
    assert batch.update == update


@freeze_time("2024-09-01")
def test__merge_same_key():
    # given
    update1 = UpdateRunSnapshot(step=Step(whole=1, micro=0), assign={f"aa{i}": Value(int64=(i * 97)) for i in range(2)})
    update2 = UpdateRunSnapshot(step=Step(whole=1, micro=0), assign={f"bb{i}": Value(int64=(i * 25)) for i in range(2)})

    # and
    operation1 = RunOperation(update=update1, project="project", run_id="run_id")
    operation2 = RunOperation(update=update2, project="project", run_id="run_id")

    # and
    element1 = SingleOperation(
        sequence_id=1,
        timestamp=time.process_time(),
        operation=operation1.SerializeToString(),
        is_metadata_update=True,
        metadata_size=update1.ByteSize(),
        operation_key=1.0,
    )
    element2 = SingleOperation(
        sequence_id=2,
        timestamp=time.process_time(),
        operation=operation2.SerializeToString(),
        is_metadata_update=True,
        metadata_size=update2.ByteSize(),
        operation_key=1.0,
    )

    # and
    queue = AggregatingQueue(max_queue_size=2, max_elements_in_batch=2)

    # and
    queue.put_nowait(element=element1)
    queue.put_nowait(element=element2)

    # when
    result = queue.get_nowait()

    # then
    assert result.sequence_id == 2
    assert result.timestamp == element2.timestamp

    # and
    batch = RunOperation()
    batch.ParseFromString(result.operation)

    assert batch.project == "project"
    assert batch.run_id == "run_id"
    assert batch.update.step == Step(whole=1, micro=0)
    assert all(k in batch.update.assign for k in ["aa0", "aa1", "bb0", "bb1"])


@freeze_time("2024-09-01")
def test__not_merge_two_different_steps():
    # given
    update1 = UpdateRunSnapshot(step=Step(whole=1, micro=0), assign={f"aa{i}": Value(int64=(i * 97)) for i in range(2)})
    update2 = UpdateRunSnapshot(step=Step(whole=2, micro=0), assign={f"bb{i}": Value(int64=(i * 25)) for i in range(2)})

    # and
    operation1 = RunOperation(update=update1, project="project", run_id="run_id")
    operation2 = RunOperation(update=update2, project="project", run_id="run_id")

    # and
    element1 = SingleOperation(
        sequence_id=1,
        timestamp=time.process_time(),
        operation=operation1.SerializeToString(),
        is_metadata_update=False,
        metadata_size=0,
        operation_key=1.0,
    )
    element2 = SingleOperation(
        sequence_id=2,
        timestamp=time.process_time(),
        operation=operation2.SerializeToString(),
        is_metadata_update=False,
        metadata_size=0,
        operation_key=2.0,
    )

    # and
    queue = AggregatingQueue(max_queue_size=2, max_elements_in_batch=2)

    # and
    queue.put_nowait(element=element1)
    queue.put_nowait(element=element2)

    # when
    result = queue.get_nowait()

    # then
    assert result.sequence_id == 1
    assert result.timestamp == element1.timestamp

    # and
    batch = RunOperation()
    batch.ParseFromString(result.operation)

    assert batch.project == "project"
    assert batch.run_id == "run_id"
    assert batch.update == update1

    # when
    result = queue.get_nowait()

    # then
    assert result.sequence_id == 2
    assert result.timestamp == element2.timestamp

    # and
    batch = RunOperation()
    batch.ParseFromString(result.operation)

    assert batch.project == "project"
    assert batch.run_id == "run_id"
    assert batch.update == update2


@freeze_time("2024-09-01")
def test__not_merge_step_with_none():
    # given
    update1 = UpdateRunSnapshot(step=Step(whole=1, micro=0), assign={f"aa{i}": Value(int64=(i * 97)) for i in range(2)})
    update2 = UpdateRunSnapshot(step=None, assign={f"bb{i}": Value(int64=(i * 25)) for i in range(2)})

    # and
    operation1 = RunOperation(update=update1, project="project", run_id="run_id")
    operation2 = RunOperation(update=update2, project="project", run_id="run_id")

    # and
    element1 = SingleOperation(
        sequence_id=1,
        timestamp=time.process_time(),
        operation=operation1.SerializeToString(),
        is_metadata_update=False,
        metadata_size=0,
        operation_key=1.0,
    )
    element2 = SingleOperation(
        sequence_id=2,
        timestamp=time.process_time(),
        operation=operation2.SerializeToString(),
        is_metadata_update=False,
        metadata_size=0,
        operation_key=None,
    )

    # and
    queue = AggregatingQueue(max_queue_size=2, max_elements_in_batch=2)

    # and
    queue.put_nowait(element=element1)
    queue.put_nowait(element=element2)

    # when
    result = queue.get_nowait()

    # then
    assert result.sequence_id == 1
    assert result.timestamp == element1.timestamp

    # and
    batch = RunOperation()
    batch.ParseFromString(result.operation)

    assert batch.project == "project"
    assert batch.run_id == "run_id"
    assert batch.update == update1

    # when
    result = queue.get_nowait()

    # then
    assert result.sequence_id == 2
    assert result.timestamp == element2.timestamp

    # and
    batch = RunOperation()
    batch.ParseFromString(result.operation)

    assert batch.project == "project"
    assert batch.run_id == "run_id"
    assert batch.update == update2
