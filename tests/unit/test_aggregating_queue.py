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
from neptune_scale.core.components.queue_element import QueueElement


@freeze_time("2024-09-01")
def test__simple():
    # given
    update = UpdateRunSnapshot(assign={f"key_{i}": Value(string=("a" * 1024)) for i in range(1024)})
    element = QueueElement(sequence_id=1, timestamp=time.process_time(), operation=RunOperation(update=update))

    # and
    queue = AggregatingQueue(max_queue_size=1)

    # when
    queue.put_nowait(element=element)

    # then
    assert queue.get_nowait() == element


@freeze_time("2024-09-01")
def test__max_size_exceeded():
    # given
    queue = AggregatingQueue(max_queue_size=1)

    # when
    queue.put_nowait(element=QueueElement(sequence_id=1, timestamp=time.process_time(), operation=RunOperation()))

    # then
    assert True

    # when
    with pytest.raises(Full):
        queue.put_nowait(
            element=QueueElement(sequence_id=2, timestamp=time.process_time() + 1, operation=RunOperation())
        )


@freeze_time("2024-09-01")
def test__empty():
    # given
    queue = AggregatingQueue(max_queue_size=1)

    # when
    with pytest.raises(Empty):
        _ = queue.get_nowait()
