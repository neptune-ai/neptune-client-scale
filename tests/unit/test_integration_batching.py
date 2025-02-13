from datetime import datetime
import threading

from freezegun import freeze_time
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import (
    Preview,
    UpdateRunSnapshot,
    Value,
)
from neptune_api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 import RunOperation
import pytest

from neptune_scale.api.attribute import AttributeStore
from neptune_scale.api.metrics import Metrics
from neptune_scale.net.serialization import (
    datetime_to_proto,
    make_step,
    make_value,
)
from neptune_scale.sync.operations_queue import OperationsQueue
from neptune_scale.sync.aggregating_queue import AggregatingQueue
from neptune_scale.sync.queue_element import SingleOperation


@pytest.mark.parametrize(
    "metrics,expected_updates",
    [
        pytest.param([
                Metrics(data={"x":1, "y":5}, step=1),
                Metrics(data={"a":2}, step=1),
            ],[
                {"step": 1, "append":{"a": 2, "x": 1, "y": 5}},
            ],
            id="Different metrics, same step",
        ),
        pytest.param([
                Metrics(data={"a":1, "b":2}, step=1),
                Metrics(data={"a":2}, step=2),
            ],[
                {"step": 1, "append":{"a": 1, "b": 2}},
                {"step": 2, "append":{"a": 2}},
            ],
            id="Different step",
        ),
        pytest.param([
                Metrics(data={"a":1, "b":2}, step=1, preview=True, preview_completion=0.2),
                Metrics(data={"a":10, "b":20}, step=1, preview=True, preview_completion=0.8),
                Metrics(data={"a":100, "b":200}, step=1),
            ],[
                {"step": 1, "append":{"a": 1, "b": 2}, "preview": True, "preview_completion": 0.2},
                {"step": 1, "append":{"a": 10, "b": 20}, "preview": True, "preview_completion": 0.8},
                {"step": 1, "append":{"a": 100, "b": 200}},
            ],
            id="Multiple previews for same point",
        ),
    ],
)
@freeze_time("2025-02-01")
def test__merge_metrics(metrics, expected_updates):
    # given
    op_queue = OperationsQueue(lock=threading.RLock(), max_size=1000)
    store = AttributeStore("project", "run_id", op_queue)
    agg_queue = AggregatingQueue(1000)

    # when
    for m in metrics:
        store.log(metrics=m)
        agg_queue.put_nowait(op_queue.queue.get())

    result = agg_queue.get()
    batch = RunOperation()
    batch.ParseFromString(result.operation)

    # then
    assert batch.project == "project"
    assert batch.run_id == "run_id"

    results = batch.update_batch.snapshots if batch.update_batch.snapshots else [batch.update]
    assert len(results) == len(expected_updates)
    for expected in expected_updates:
        preview = Preview(is_preview=expected["preview"], completion_ratio=expected.get("preview_completion", 0.0)) if "preview" in expected else None
        exp_proto = UpdateRunSnapshot(
            step=make_step(expected.get("step")),
            timestamp=datetime_to_proto(datetime.now()),
            append={k: make_value(float(v)) for k,v in expected.get("append", {}).items()},
            preview=preview,
        )
        for got in results:
            if got == exp_proto:
                break
        else:
            pytest.fail(f"didn't find expected result: {exp_proto}")
