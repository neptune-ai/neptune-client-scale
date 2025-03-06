import time
from unittest.mock import Mock

from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import Run as CreateRun
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
    UpdateRunSnapshot,
    Value,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale.sync.operations_repository import (
    Operation,
    OperationType,
    SequenceId,
)
from neptune_scale.sync.parameters import MAX_REQUEST_SIZE_BYTES
from neptune_scale.sync.sync_process import _stream_operations as stream_operations


def create_operation(sequence_id: int, operation_type: OperationType) -> Operation:
    if operation_type == OperationType.CREATE_RUN:
        operation = CreateRun(family="test-run-id", experiment_id="Test Run")
    else:
        operation = UpdateRunSnapshot(assign={"key": Value(string=f"value_{sequence_id}")})

    return Operation(
        sequence_id=SequenceId(sequence_id),
        timestamp=int(time.time() * 1000),
        operation_type=operation_type,
        operation=operation,
        operation_size_bytes=operation.ByteSize(),
    )


def test_stream_operations_empty_operations():
    operations_repository = Mock()
    operations_repository.get_operations.return_value = []

    generator = stream_operations(operations_repository, "test_run_id", "test_project", MAX_REQUEST_SIZE_BYTES)
    assert list(generator) == []


def test_stream_operations_create_run_only():
    # Arrange
    operations_repository = Mock()
    create_run_op = create_operation(1, OperationType.CREATE_RUN)
    operations_repository.get_operations.side_effect = [[create_run_op], []]

    generator = stream_operations(operations_repository, "test_run_id", "test_project", MAX_REQUEST_SIZE_BYTES)
    result = next(generator)

    assert isinstance(result, tuple)
    assert len(result) == 3
    run_operation, sequence_id, timestamp = result

    assert isinstance(run_operation, RunOperation)
    assert run_operation.project == "test_project"
    assert run_operation.run_id == "test_run_id"
    assert hasattr(run_operation, "create")
    assert sequence_id == 1

    # The generator should exit after this
    assert list(generator) == []


def test_stream_operations_update_operations_only():
    operations_repository = Mock()
    update_op1 = create_operation(1, OperationType.UPDATE_SNAPSHOT)
    update_op2 = create_operation(2, OperationType.UPDATE_SNAPSHOT)
    operations_repository.get_operations.side_effect = [[update_op1, update_op2], []]

    generator = stream_operations(operations_repository, "test_run_id", "test_project", MAX_REQUEST_SIZE_BYTES)
    result = next(generator)

    assert isinstance(result, tuple)
    assert len(result) == 3
    run_operation, sequence_id, timestamp = result

    assert isinstance(run_operation, RunOperation)
    assert run_operation.project == "test_project"
    assert run_operation.run_id == "test_run_id"
    assert hasattr(run_operation, "update_batch")
    assert sequence_id == 2  # Should be the last operation's sequence_id

    # The generator should exit after this
    assert list(generator) == []


def test_stream_operations_create_run_and_updates():
    operations_repository = Mock()
    create_run_op = create_operation(1, OperationType.CREATE_RUN)
    update_op1 = create_operation(2, OperationType.UPDATE_SNAPSHOT)
    update_op2 = create_operation(3, OperationType.UPDATE_SNAPSHOT)
    operations_repository.get_operations.side_effect = [[create_run_op, update_op1, update_op2], []]

    generator = stream_operations(operations_repository, "test_run_id", "test_project", MAX_REQUEST_SIZE_BYTES)

    # First yield should be the CREATE_RUN operation
    result1 = next(generator)
    assert isinstance(result1, tuple)
    run_operation1, sequence_id1, timestamp1 = result1
    assert hasattr(run_operation1, "create")
    assert sequence_id1 == 1

    # Second yield should be the batch of update operations
    result2 = next(generator)
    assert isinstance(result2, tuple)
    run_operation2, sequence_id2, timestamp2 = result2
    assert hasattr(run_operation2, "update_batch")
    assert sequence_id2 == 3  # Should be the last operation's sequence_id

    # The generator should exit after this
    assert list(generator) == []


def test_stream_operations_multiple_batches():
    operations_repository = Mock()
    create_run_op = create_operation(1, OperationType.CREATE_RUN)
    update_op1 = create_operation(2, OperationType.UPDATE_SNAPSHOT)
    update_op2 = create_operation(3, OperationType.UPDATE_SNAPSHOT)
    update_op3 = create_operation(4, OperationType.UPDATE_SNAPSHOT)
    operations_repository.get_operations.side_effect = [[create_run_op, update_op1], [update_op2, update_op3], []]

    generator = stream_operations(operations_repository, "test_run_id", "test_project", MAX_REQUEST_SIZE_BYTES)

    # First yield should be the CREATE_RUN operation
    result1 = next(generator)
    assert isinstance(result1, tuple)
    run_operation1, sequence_id1, timestamp1 = result1
    assert hasattr(run_operation1, "create")
    assert sequence_id1 == 1

    # Second yield should be the first update operation
    result2 = next(generator)
    assert isinstance(result2, tuple)
    run_operation2, sequence_id2, timestamp2 = result2
    assert hasattr(run_operation2, "update_batch")
    assert sequence_id2 == 2

    # Third yield should be the second batch of update operations
    result3 = next(generator)
    assert isinstance(result3, tuple)
    run_operation3, sequence_id3, timestamp3 = result3
    assert hasattr(run_operation3, "update_batch")
    assert sequence_id3 == 4

    # The generator should exit after this
    assert list(generator) == []
