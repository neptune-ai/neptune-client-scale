import time
from queue import (
    Empty,
    Full,
)

import pytest
from freezegun import freeze_time
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
    UpdateRunSnapshot,
    Value,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale.core.components.aggregating_queue import AggregatingQueue
from neptune_scale.core.components.queue_element import BatchedOperations


@freeze_time("2024-09-01")
def test__simple():
    # given
    update = UpdateRunSnapshot(assign={f"key_{i}": Value(string=("a" * 2)) for i in range(2)})
    operation = RunOperation(update=update)
    element = BatchedOperations(sequence_id=1, timestamp=time.process_time(), operation=operation.SerializeToString())

    # and
    queue = AggregatingQueue(max_queue_size=1)

    # when
    queue.put_nowait(element=element)

    # then
    assert queue.get_nowait() == element


@freeze_time("2024-09-01")
def test__max_size_exceeded():
    # given
    operation1 = RunOperation()
    operation2 = RunOperation()
    element1 = BatchedOperations(sequence_id=1, timestamp=time.process_time(), operation=operation1.SerializeToString())
    element2 = BatchedOperations(sequence_id=2, timestamp=time.process_time(), operation=operation2.SerializeToString())

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
    element1 = BatchedOperations(sequence_id=1, timestamp=time.process_time(), operation=operation1.SerializeToString())
    element2 = BatchedOperations(sequence_id=2, timestamp=time.process_time(), operation=operation2.SerializeToString())

    # and
    queue = AggregatingQueue(max_queue_size=2, max_elements_in_batch=1)

    # when
    queue.put_nowait(element=element1)
    queue.put_nowait(element=element2)

    # then
    assert queue.get_nowait() == element1
    assert queue.get_nowait() == element2


@freeze_time("2024-09-01")
def test__batching():
    # given
    update1 = UpdateRunSnapshot(step=None, assign={f"aa{i}": Value(int64=(i * 97)) for i in range(2)})
    update2 = UpdateRunSnapshot(step=None, assign={f"bb{i}": Value(int64=(i * 25)) for i in range(2)})

    # and
    operation1 = RunOperation(update=update1, project="project", run_id="run_id")
    operation2 = RunOperation(update=update2, project="project", run_id="run_id")

    # and
    element1 = BatchedOperations(sequence_id=1, timestamp=time.process_time(), operation=operation1.SerializeToString())
    element2 = BatchedOperations(sequence_id=2, timestamp=time.process_time(), operation=operation2.SerializeToString())

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
