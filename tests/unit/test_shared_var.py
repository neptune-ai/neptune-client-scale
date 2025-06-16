import multiprocessing
from multiprocessing import Process

import pytest

from neptune_scale.util import (
    SharedFloat,
    SharedInt,
)

mp_context = multiprocessing.get_context("spawn")


TIMEOUT = 10


def _child(var):
    with var:
        var.value = 1
        var.notify_all()

    var.wait(timeout=TIMEOUT)
    assert var.value == 2


@pytest.mark.parametrize("tp", (SharedInt, SharedFloat))
def test_set_and_notify(tp):
    var = tp(multiprocessing_context=mp_context, initial_value=0)

    process = Process(target=_child, args=(var,))
    process.start()

    var.wait(timeout=TIMEOUT)
    assert var.value == 1

    with var:
        var.value = 2
        var.notify_all()

    process.join(timeout=TIMEOUT)

    assert var.value == 2
    assert process.exitcode == 0, "Child process failed"
