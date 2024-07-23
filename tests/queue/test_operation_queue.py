from datetime import datetime
from threading import Timer
from typing import (
    List,
    Optional,
    Tuple,
)

import pytest

from neptune_scale.queue.operation_queue import (
    BLOCK_TIMEOUT,
    OperationQueue,
)

WAIT_TIME = 1.0
WAIT_ERROR = WAIT_TIME * 0.1 + BLOCK_TIMEOUT
TIMEOUT = WAIT_TIME + BLOCK_TIMEOUT + WAIT_ERROR


def test_put():
    queue = OperationQueue[int, int](batch_size=5)

    values = [1, 2, 3, 4, 5]
    for value in values:
        queue.put(key=1, value=value)

    batch = queue.get_batch()
    assert batch == [(1, 1), (1, 2), (1, 3), (1, 4), (1, 5)]


@pytest.mark.timeout(TIMEOUT)
def test_blocking_get():
    queue = OperationQueue[int, int](batch_size=4)

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


@pytest.mark.timeout(TIMEOUT)
@pytest.mark.parametrize(
    ("category_series", "expected_batch_summary"),
    [
        ([1, 1, 1, 1, 1], [(5, 1)]),
        ([1, 1, None, None, 1, 1], [(2, 1), (1, None), (1, None), (2, 1)]),
        ([1, 1, 1, 2, 2, 2], [(3, 1), (3, 2)]),
        ([1, 1, 1, None, None, 2, 2, 2], [(3, 1), (1, None), (1, None), (3, 2)]),
        ([None, None, None, 1], [(1, None), (1, None), (1, None), (1, 1)]),
        ([None, None, None, None], [(1, None), (1, None), (1, None), (1, None)]),
    ],
)
def test_batching(category_series: List[Optional[int]], expected_batch_summary: List[Tuple[int, int]]):
    values = [i for i in range(len(category_series))]
    queue = OperationQueue[int, int](batch_size=64)
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
    queue = OperationQueue[int, int](batch_size=64)
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
