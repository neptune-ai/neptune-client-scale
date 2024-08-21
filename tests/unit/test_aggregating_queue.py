import random
import string
from datetime import datetime
from threading import Timer
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
)

import pytest
from freezegun import freeze_time
from google.protobuf.timestamp_pb2 import Timestamp
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
    Run,
    UpdateRunSnapshot,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation

from neptune_scale.core.components.aggregating_queue import (
    DEFAULT_TIMEOUT,
    AggregatingQueue,
)
from neptune_scale.core.components.queue_element import QueueElement

WAIT_TIME = 1.0
WAIT_ERROR = WAIT_TIME * 0.1 + DEFAULT_TIMEOUT
TIMEOUT = WAIT_TIME + DEFAULT_TIMEOUT + WAIT_ERROR


from neptune_scale.core.serialization import (
    datetime_to_proto,
    make_step,
    make_value,
)


def build_update_op(
    project: str,
    run_id: str,
    step: Optional[int] = None,
    assign: Optional[Dict[str, Any]] = None,
    append: Optional[Dict[str, Any]] = None,
    modify_sets: Optional[Dict[str, Any]] = None,
) -> RunOperation:
    assign = assign or {}
    append = append or {}
    modify_sets = modify_sets or {}

    return RunOperation(
        project=project,
        run_id=run_id,
        update=UpdateRunSnapshot(
            step=make_step(step) if step is not None else None,
            timestamp=datetime_to_proto(datetime.now()),
            assign={k: make_value(v) for k, v in assign.items()},
            append={k: make_value(v) for k, v in append.items()},
            modify_sets={k: make_value(v) for k, v in modify_sets.items()},
        ),
    )


def build_create_op(
    project: str,
    run_id: str,
    family: str,
    experiment_id: Optional[str] = None,
) -> RunOperation:
    timestamp: Timestamp = Timestamp()
    timestamp.GetCurrentTime()
    return RunOperation(
        project=project,
        run_id=run_id,
        create=Run(
            creation_time=timestamp,
            experiment_id=experiment_id,
            family=family,
        ),
    )


@freeze_time("2024-07-20 12:12:12.000022")
def test_put():
    queue = AggregatingQueue(max_batch_size=5)
    value_names = ["a", "b", "c", "d", "e"]
    values = [1, 2, 3, 4, 5]

    for name, value in zip(value_names, values):
        queue.put(
            QueueElement(
                sequence_id=1,
                timestamp=1.0,
                operation=build_update_op("project", "run_id", 1, assign={name: value}).SerializeToString(),
            )
        )

    aggregate = queue.get_aggregate()
    serialized_operation = aggregate.operation
    aggregate_op: RunOperation = RunOperation()
    aggregate_op.ParseFromString(serialized_operation)

    expected_operation = build_update_op("project", "run_id", 1, assign=dict(zip(value_names, values)))

    assert aggregate.sequence_id, aggregate.timestamp == (1, 1.0)
    assert aggregate_op == expected_operation


@pytest.mark.timeout(TIMEOUT)
@freeze_time("2024-07-20 12:12:12.000022")
def test_blocking_get():
    queue = AggregatingQueue(max_batch_size=4)

    def complete_batch():
        queue.put(QueueElement(0, 0, build_update_op("project", "run_id", 1, append={"d": 4}).SerializeToString()))

    t = Timer(WAIT_TIME, complete_batch)
    appends = [("a", 1), ("b", 2), ("c", 3)]
    for k, v in appends:
        queue.put(QueueElement(0, 0, build_update_op("project", "run_id", 1, append={k: v}).SerializeToString()))

    block_time = datetime.now()
    t.start()
    aggregated_elements = queue.get_aggregate()
    unblock_time = datetime.now()

    aggregated_op: RunOperation = RunOperation()
    aggregated_op.ParseFromString(aggregated_elements.operation)

    assert abs((unblock_time - block_time).total_seconds() - WAIT_TIME) <= WAIT_ERROR
    assert aggregated_op == build_update_op("project", "run_id", 1, append=dict(appends + [("d", 4)]))


# positive values represent step valueo of update operation
# None values represent update operations with no step values
# negative values represent create operations with run_id set to given value
@pytest.mark.timeout(TIMEOUT)
@freeze_time("2024-07-20 12:12:12.000022")
@pytest.mark.parametrize(
    ("category_series", "expected_batch_summary"),
    [
        ([1, 1, 1, 1, 1], [(5, 1)]),
        ([1, 1, None, None, 1, 1], [(2, 1), (2, None), (2, 1)]),
        ([1, 1, 1, 2, 2, 2], [(3, 1), (3, 2)]),
        ([1, 1, 1, None, None, 2, 2, 2], [(3, 1), (2, None), (3, 2)]),
        ([None, None, 1, 1], [(2, None), (2, 1)]),
        ([None, None, None, None], [(4, None)]),
        ([-1, -1, -1], [(1, -1), (1, -1), (1, -1)]),
        ([-1, 1, 1, None, None, -1, 2], [(1, -1), (2, 1), (2, None), (1, -1), (1, 2)]),
    ],
)
def test_batching(category_series: List[int], expected_batch_summary: List[Tuple[int, int]]):
    queue = AggregatingQueue(max_batch_size=64)
    queue.stop_blocking()

    for i, step in enumerate(category_series):
        if step is None or step >= 0:
            k = string.ascii_lowercase[i]
            queue_element = QueueElement(
                0, 0, build_update_op("project", "run_id", step=step, assign={k: 1}).SerializeToString()
            )
        else:
            queue_element = QueueElement(
                0, 0, build_create_op("project", "run_id", "fam", experiment_id=f"id={step}").SerializeToString()
            )

        queue.put(queue_element)

    for expected_assigns, expected_step in expected_batch_summary:
        run_op = RunOperation()
        run_op.ParseFromString(queue.get_aggregate().operation)

        if isinstance(expected_step, int) and expected_step >= 0:
            assert len(run_op.update.assign) == expected_assigns
            assert run_op.update.step == make_step(expected_step)
        elif isinstance(expected_step, int) and expected_step < 0:
            assert run_op.HasField("create")
            assert run_op.create.experiment_id == f"id={expected_step}"
        else:
            assert not run_op.update.HasField("step")


@pytest.mark.timeout(TIMEOUT)
def test_stop_blocking():
    queue = AggregatingQueue(max_batch_size=64)
    updates = [("a", 1), ("b", 2), ("c", 3)]
    for k, v in updates:
        queue.put(QueueElement(0, 0, build_update_op("project", "run_id", 1, assign={k: v}).SerializeToString()))

    t = Timer(WAIT_TIME, queue.stop_blocking)
    t.start()

    block_time = datetime.now()
    aggregate = queue.get_aggregate()
    unblock_time = datetime.now()

    run_op = RunOperation()
    run_op.ParseFromString(aggregate.operation)

    assert abs((unblock_time - block_time).total_seconds() - WAIT_TIME) <= WAIT_ERROR
    assert sorted([(k, v.int64) for k, v in run_op.update.assign.items()]) == sorted(updates)


def test_non_blocking():
    queue = AggregatingQueue(max_batch_size=64, block=False)
    updates = [("a", 1), ("b", 2), ("c", 3)]
    for k, v in updates:
        queue.put(QueueElement(0, 0, build_update_op("project", "run_id", 1, assign={k: v}).SerializeToString()))

    aggregate = queue.get_aggregate()
    run_op = RunOperation()
    run_op.ParseFromString(aggregate.operation)

    assert sorted([(k, v.int64) for k, v in run_op.update.assign.items()]) == sorted(updates)
    assert queue.get_aggregate() is None


def test_max_aggregate_size():
    rand_str = "".join(random.choices(string.ascii_lowercase, k=1000))
    op_1 = build_update_op("project", "run_id", 1, assign={"small": 1})
    op_2 = build_update_op("project", "run_id", 1, assign={f"{rand_str}_big": 1})

    queue = AggregatingQueue(max_batch_size=64, block=False, batch_max_bytes=op_2.ByteSize())
    queue.put(QueueElement(0, 0, op_1.SerializeToString()))
    queue.put(QueueElement(0, 0, op_2.SerializeToString()))

    run_op = RunOperation()

    aggregate1 = queue.get_aggregate()
    aggregate2 = queue.get_aggregate()
    aggregate3 = queue.get_aggregate()
    assert aggregate1 is not None
    assert aggregate2 is not None
    assert aggregate3 is None

    run_op.ParseFromString(aggregate1.operation)
    assert run_op == op_1

    run_op.ParseFromString(aggregate2.operation)
    assert run_op == op_2


def test_queue_element_info_retention():
    queue = AggregatingQueue(max_batch_size=2)
    op = build_update_op("project", "run_id", 1, assign={"a": 1})
    op.update.timestamp.FromJsonString("2024-01-01T00:00:00Z")
    queue.put(QueueElement(sequence_id=1, timestamp=1, operation=op.SerializeToString()))

    op.update.timestamp.FromJsonString("2025-01-01T00:00:00Z")
    queue.put(QueueElement(sequence_id=2, timestamp=2, operation=op.SerializeToString()))

    aggregate = queue.get_aggregate()
    assert aggregate.sequence_id == 2
    assert aggregate.timestamp == 2

    run_op = RunOperation()
    run_op.ParseFromString(aggregate.operation)
    assert run_op.update.timestamp.ToJsonString() == "2025-01-01T00:00:00Z"
