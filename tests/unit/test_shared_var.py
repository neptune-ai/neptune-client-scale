from multiprocessing import Process

import pytest

from neptune_scale.core.shared_var import (
    SharedFloat,
    SharedInt,
)


def _child(var):
    with var:
        var.value = 1
        var.notify_all()

    var.wait(timeout=1)
    assert var.value == 2


@pytest.mark.parametrize("tp", (SharedInt, SharedFloat))
def test_set_and_notify(tp):
    var = tp(0)

    process = Process(target=_child, args=(var,))
    process.start()

    var.wait(timeout=1)
    assert var.value == 1

    with var:
        var.value = 2
        var.notify_all()

    process.join()

    assert var.value == 2
    assert process.exitcode == 0, "Child process failed"
