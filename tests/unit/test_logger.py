from concurrent.futures.process import ProcessPoolExecutor
from unittest.mock import Mock

import pytest

from neptune_scale.core.logger import (
    get_logger,
    init_child_process_logger,
    init_main_process_logger,
)


def test_multiple_initialization_in_main_process():
    logger, queue = init_main_process_logger()

    # Keep a copy of handlers to make sure they're not modified once the logger is initialized.
    handlers = logger.handlers[:]
    _, queue2 = init_main_process_logger()

    assert queue is queue2
    assert get_logger().handlers == handlers


def test_restrict_main_process_initialization_to_main_process():
    with ProcessPoolExecutor() as executor, pytest.raises(RuntimeError) as err:
        executor.submit(init_main_process_logger).result()

    err.match("only in the main process")


def test_restrict_child_process_initialization_to_child_process():
    with pytest.raises(RuntimeError) as err:
        init_child_process_logger(Mock())

    err.match("only in child process")


def _child():
    logger = init_child_process_logger(Mock())

    # Keep a copy of handlers to make sure they're not modified once the logger is initialized.
    handlers = logger.handlers[:]
    init_child_process_logger(Mock())

    assert get_logger().handlers == handlers


def test_multiple_initialization_in_child_process():
    with ProcessPoolExecutor() as executor:
        executor.submit(_child).result()
