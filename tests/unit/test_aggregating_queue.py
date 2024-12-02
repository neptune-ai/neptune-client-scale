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

from neptune_scale.sync.aggregating_queue import AggregatingQueue
from neptune_scale.sync.queue_element import (
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
        is_batchable=True,
        metadata_size=update.ByteSize(),
        step=None,
    )

    # and
    queue = AggregatingQueue(max_queue_size=1)

    # when
    queue.put_nowait(element=element)

    # then
    assert queue.get() == BatchedOperations(
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
        is_batchable=True,
        metadata_size=0,
        step=None,
    )
    element2 = SingleOperation(
        sequence_id=2,
        timestamp=time.process_time(),
        operation=operation2.SerializeToString(),
        is_batchable=True,
        metadata_size=0,
        step=None,
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
        _ = queue.get()


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
        is_batchable=True,
        metadata_size=update1.ByteSize(),
        step=None,
    )
    element2 = SingleOperation(
        sequence_id=2,
        timestamp=time.process_time(),
        operation=operation2.SerializeToString(),
        is_batchable=True,
        metadata_size=update2.ByteSize(),
        step=None,
    )

    # and
    queue = AggregatingQueue(max_queue_size=2, max_elements_in_batch=1)

    # when
    queue.put_nowait(element=element1)
    queue.put_nowait(element=element2)

    # then
    assert queue.get() == BatchedOperations(sequence_id=1, timestamp=element1.timestamp, operation=element1.operation)
    assert queue.get() == BatchedOperations(sequence_id=2, timestamp=element2.timestamp, operation=element2.operation)


@freeze_time("2024-09-01")
def test__batching_with_step():
    # given
    update1 = UpdateRunSnapshot(step=None, assign={f"aa{i}": Value(int64=(i * 97)) for i in range(2)})
    update2 = UpdateRunSnapshot(step=None, assign={f"bb{i}": Value(int64=(i * 25)) for i in range(2)})

    # and
    operation1 = RunOperation(update=update1, project="project", run_id="run_id")
    operation2 = RunOperation(update=update2, project="project", run_id="run_id")

    # and
    element1 = SingleOperation(
        sequence_id=1,
        timestamp=time.monotonic(),
        operation=operation1.SerializeToString(),
        is_batchable=True,
        metadata_size=update1.ByteSize(),
        step=1,
    )
    element2 = SingleOperation(
        sequence_id=2,
        timestamp=time.monotonic(),
        operation=operation2.SerializeToString(),
        is_batchable=True,
        metadata_size=update2.ByteSize(),
        step=1,
    )

    # and
    queue = AggregatingQueue(max_queue_size=2, max_elements_in_batch=2)

    # and
    queue.put_nowait(element=element1)
    queue.put_nowait(element=element2)

    # when
    result = queue.get()

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
def test__batch_metrics_no_step():
    """
    Log a single metric 2 times, with step being None. The two operations should be batched, but not merged.
    """

    # given
    updates = [UpdateRunSnapshot(step=None, append={"a": Value(int64=value)}) for value in range(2)]

    # and
    operations = [RunOperation(update=update, project="project", run_id="run_id") for update in updates]

    elements = [
        SingleOperation(
            sequence_id=seq,
            timestamp=time.monotonic(),
            operation=operations[seq].SerializeToString(),
            is_batchable=True,
            metadata_size=updates[seq].ByteSize(),
            step=None,
        )
        for seq in range(2)
    ]

    # and
    queue = AggregatingQueue(max_queue_size=10, max_elements_in_batch=10, wait_time=0.1)

    # and
    for e in elements:
        queue.put_nowait(element=e)

    # when
    result = queue.get()

    # then
    assert result.sequence_id == elements[-1].sequence_id
    assert result.timestamp == elements[-1].timestamp

    batch = RunOperation()
    batch.ParseFromString(result.operation)

    assert batch.project == "project"
    assert batch.run_id == "run_id"
    assert len(batch.update_batch.snapshots) == len(operations)

    for i, update in enumerate(batch.update_batch.snapshots):
        assert update.append == {"a": Value(int64=i)}


@freeze_time("2024-09-01")
def test__queue_element_size_limit_with_different_steps():
    # given
    update1 = UpdateRunSnapshot(step=Step(whole=1), assign={f"aa{i}": Value(int64=(i * 97)) for i in range(2)})
    update2 = UpdateRunSnapshot(step=Step(whole=2), assign={f"bb{i}": Value(int64=(i * 25)) for i in range(2)})
    operation1 = RunOperation(update=update1)
    operation2 = RunOperation(update=update2)
    element1 = SingleOperation(
        sequence_id=1,
        timestamp=time.process_time(),
        operation=operation1.SerializeToString(),
        is_batchable=True,
        metadata_size=update1.ByteSize(),
        step=1.0,
    )
    element2 = SingleOperation(
        sequence_id=2,
        timestamp=time.process_time(),
        operation=operation2.SerializeToString(),
        is_batchable=True,
        metadata_size=update2.ByteSize(),
        step=2.0,
    )

    # and
    queue = AggregatingQueue(max_queue_size=2, max_queue_element_size=update1.ByteSize())

    # when
    queue.put_nowait(element=element1)
    queue.put_nowait(element=element2)

    # then
    assert queue.get() == BatchedOperations(sequence_id=1, timestamp=element1.timestamp, operation=element1.operation)
    assert queue.get() == BatchedOperations(sequence_id=2, timestamp=element2.timestamp, operation=element2.operation)


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
        is_batchable=False,
        metadata_size=0,
        step=None,
    )
    element2 = SingleOperation(
        sequence_id=2,
        timestamp=time.process_time(),
        operation=operation2.SerializeToString(),
        is_batchable=False,
        metadata_size=0,
        step=None,
    )

    # and
    queue = AggregatingQueue(max_queue_size=2, max_elements_in_batch=2)

    # and
    queue.put_nowait(element=element1)
    queue.put_nowait(element=element2)

    # when
    result = queue.get()

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
    result = queue.get()

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
        is_batchable=False,
        metadata_size=0,
        step=None,
    )
    element2 = SingleOperation(
        sequence_id=2,
        timestamp=time.process_time(),
        operation=operation2.SerializeToString(),
        is_batchable=True,
        metadata_size=update.ByteSize(),
        step=None,
    )

    # and
    queue = AggregatingQueue(max_queue_size=2, max_elements_in_batch=2)

    # and
    queue.put_nowait(element=element1)
    queue.put_nowait(element=element2)

    # when
    result = queue.get()

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
    result = queue.get()

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
        is_batchable=True,
        metadata_size=update1.ByteSize(),
        step=1.0,
    )
    element2 = SingleOperation(
        sequence_id=2,
        timestamp=time.process_time(),
        operation=operation2.SerializeToString(),
        is_batchable=True,
        metadata_size=update2.ByteSize(),
        step=1.0,
    )

    # and
    queue = AggregatingQueue(max_queue_size=2, max_elements_in_batch=2)

    # and
    queue.put_nowait(element=element1)
    queue.put_nowait(element=element2)

    # when
    result = queue.get()

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
def test__merge_two_different_steps():
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
        is_batchable=True,
        metadata_size=0,
        step=1.0,
    )
    element2 = SingleOperation(
        sequence_id=2,
        timestamp=time.process_time(),
        operation=operation2.SerializeToString(),
        is_batchable=True,
        metadata_size=0,
        step=2.0,
    )

    # and
    queue = AggregatingQueue(max_queue_size=2, max_elements_in_batch=2)

    # and
    queue.put_nowait(element=element1)
    queue.put_nowait(element=element2)

    # when
    result = queue.get()

    # then
    assert result.sequence_id == element2.sequence_id
    assert result.timestamp == element2.timestamp

    # and
    batch = RunOperation()
    batch.ParseFromString(result.operation)

    assert batch.project == "project"
    assert batch.run_id == "run_id"
    assert batch.update_batch.snapshots == [update1, update2]


@freeze_time("2024-09-01")
def test__merge_step_with_none():
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
        is_batchable=True,
        metadata_size=0,
        step=1.0,
    )
    element2 = SingleOperation(
        sequence_id=2,
        timestamp=time.process_time(),
        operation=operation2.SerializeToString(),
        is_batchable=True,
        metadata_size=0,
        step=None,
    )

    # and
    queue = AggregatingQueue(max_queue_size=2, max_elements_in_batch=2)

    # and
    queue.put_nowait(element=element1)
    queue.put_nowait(element=element2)

    # when
    result = queue.get()

    # then
    assert result.sequence_id == element2.sequence_id
    assert result.timestamp == element2.timestamp

    # and
    batch = RunOperation()
    batch.ParseFromString(result.operation)

    assert batch.project == "project"
    assert batch.run_id == "run_id"
    assert batch.update_batch.snapshots == [update1, update2]
