from datetime import datetime
from threading import Timer
from typing import (
    List,
    Tuple,
)

import pytest

from neptune_scale.queue.batching_queue import (
    DEFAULT_TIMEOUT,
    BatchingQueue,
)

WAIT_TIME = 1.0
WAIT_ERROR = WAIT_TIME * 0.1 + DEFAULT_TIMEOUT
TIMEOUT = WAIT_TIME + DEFAULT_TIMEOUT + WAIT_ERROR


def _sign(x: float) -> int:
    return 1 if x >= 0 else -1


def test_put():
    queue = BatchingQueue[int, int](batch_size=5, is_batchable_fn=lambda x: True)

    values = [1, 2, 3, 4, 5]
    for value in values:
        queue.put(key=1, value=value)

    batch = queue.get_batch()
    assert batch == [(1, 1), (1, 2), (1, 3), (1, 4), (1, 5)]


@pytest.mark.timeout(TIMEOUT)
def test_blocking_get():
    queue = BatchingQueue[int, int](batch_size=4, is_batchable_fn=lambda x: True)

    def complete_batch():
        queue.put(key=1, value=4)

    t = Timer(WAIT_TIME, complete_batch)
    values = [1, 2, 3]
    for value in values:
        queue.put(key=1, value=value)

    block_time = datetime.now()
    t.start()
    batch = queue.get_batch()
    unblock_time = datetime.now()

    assert abs((unblock_time - block_time).total_seconds() - WAIT_TIME) <= WAIT_ERROR
    assert batch == [(1, 1), (1, 2), (1, 3), (1, 4)]


# negative values should not be batched
@pytest.mark.timeout(TIMEOUT)
@pytest.mark.parametrize(
    ("category_series", "expected_batch_summary"),
    [
        ([1, 1, 1, 1, 1], [(5, 1)]),
        ([1, 1, -1, -1, 1, 1], [(2, 1), (1, -1), (1, -1), (2, 1)]),
        ([1, 1, 1, 2, 2, 2], [(3, 1), (3, 2)]),
        ([1, 1, 1, -2, -2, 2, 2, 2], [(3, 1), (1, -2), (1, -2), (3, 2)]),
        ([-1, -2, 1, 1], [(1, -1), (1, -2), (2, 1)]),
        ([-1, -1, -1, -1], [(1, -1), (1, -1), (1, -1), (1, -1)]),
    ],
)
def test_batching(category_series: List[int], expected_batch_summary: List[Tuple[int, int]]):
    values = [i * _sign(v) for i, v in enumerate(category_series)]
    queue = BatchingQueue[int, int](batch_size=64, is_batchable_fn=lambda x: x >= 0)
    queue.stop_blocking()

    for k, v in zip(category_series, values):
        queue.put(key=k, value=v)

    offset = 0
    for expected_batch_size, expected_batch_key in expected_batch_summary:
        batch = queue.get_batch()
        assert len(batch) == expected_batch_size
        assert all(key == expected_batch_key for key, _ in batch)
        batch_values = [v for _, v in batch]
        assert batch_values == values[offset : offset + expected_batch_size]
        offset += expected_batch_size


@pytest.mark.timeout(TIMEOUT)
def test_stop_blocking():
    queue = BatchingQueue[int, int](batch_size=64, is_batchable_fn=lambda x: True)
    values = [1, 2, 3]
    for value in values:
        queue.put(key=1, value=value)

    t = Timer(WAIT_TIME, queue.stop_blocking)
    t.start()

    block_time = datetime.now()
    batch = queue.get_batch()
    unblock_time = datetime.now()

    assert abs((unblock_time - block_time).total_seconds() - WAIT_TIME) <= WAIT_ERROR
    assert batch == [(1, 1), (1, 2), (1, 3)]


def test_non_blocking():
    queue = BatchingQueue[int, int](batch_size=64, block=False, is_batchable_fn=lambda x: True)
    values = [1, 2, 3]
    for value in values:
        queue.put(key=1, value=value)

    batch = queue.get_batch()
    assert batch == [(1, 1), (1, 2), (1, 3)]
    assert queue.get_batch() == []
