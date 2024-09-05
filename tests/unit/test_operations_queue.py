import threading

import pytest
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
    UpdateRunSnapshot,
    Value,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale.core.components.operations_queue import OperationsQueue


def test__enqueue():
    # given
    lock = threading.RLock()
    queue = OperationsQueue(lock=lock, max_size=0)

    # and
    operation = RunOperation()

    # when
    queue.enqueue(operation=operation, metadata_size=0)

    # then
    assert queue._sequence_id == 1

    # when
    queue.enqueue(operation=operation, metadata_size=0)

    # then
    assert queue._sequence_id == 2


def test__max_element_size_exceeded():
    # given
    lock = threading.RLock()
    queue = OperationsQueue(lock=lock, max_size=1)

    # and
    snapshot = UpdateRunSnapshot(assign={f"key_{i}": Value(string=("a" * 1024)) for i in range(1024)})
    operation = RunOperation(update=snapshot)

    # then
    with pytest.raises(ValueError):
        queue.enqueue(operation=operation, metadata_size=snapshot.ByteSize())
