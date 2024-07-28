import threading

from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale.core.components.operations_queue import OperationsQueue


def test_operations_queue():
    # given
    lock = threading.RLock()
    queue = OperationsQueue(lock=lock, max_size=0)

    # and
    operation = RunOperation()

    # when
    queue.enqueue(operation=operation)

    # then
    assert queue._sequence_id == 1

    # when
    queue.enqueue(operation=operation)

    # then
    assert queue._sequence_id == 2
