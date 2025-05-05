import math
import time

import pytest

from neptune_scale.util.timer import Timer


@pytest.mark.parametrize(
    "timeout, sleep, remaining_time",
    (
        (None, 0, None),
        (None, 1, None),
        (1, 0, 1),
        (1, 0.5, 0.5),
        (1, 1, 0),
        (1, 2, 0),
    ),
)
def test_timer_remaining_time(timeout, sleep, remaining_time):
    timer = Timer(timeout)

    time.sleep(sleep)

    if remaining_time is None:
        assert timer.remaining_time() is None
    else:
        assert timer.remaining_time() <= remaining_time


@pytest.mark.parametrize(
    "timeout, sleep, remaining_time",
    (
        (None, 0, None),
        (None, 1, None),
        (1, 0, 1),
        (1, 0.5, 0.5),
        (1, 1, 0),
        (1, 2, 0),
    ),
)
def test_timer_remaining_time_or_inf(timeout, sleep, remaining_time):
    timer = Timer(timeout)

    time.sleep(sleep)

    if remaining_time is None:
        assert timer.remaining_time_or_inf() == math.inf
    else:
        assert timer.remaining_time_or_inf() <= remaining_time


@pytest.mark.parametrize(
    "timeout, sleep, is_expired",
    (
        (None, 0, False),
        (None, 1, False),
        (2, 0, False),
        (2, 0.5, False),
        (1, 1, True),
        (1, 2, True),
    ),
)
def test_timer_is_expired(timeout, sleep, is_expired):
    timer = Timer(timeout)

    time.sleep(sleep)

    assert timer.is_expired() == is_expired


@pytest.mark.parametrize(
    "timeout, is_finite",
    (
        (None, False),
        (0, True),
        (1, True),
        (2, True),
    ),
)
def test_timer_is_finite(timeout, is_finite):
    timer = Timer(timeout)

    assert timer.is_finite() == is_finite
