import logging
from datetime import datetime
from unittest.mock import Mock

import pytest
from freezegun import freeze_time

from neptune_scale import Run
from neptune_scale.logging.logging_handler import NeptuneLoggingHandler
from neptune_scale.sync.parameters import MAX_ATTRIBUTE_PATH_LENGTH

LINE_LIMIT = 1024 * 1024


# The test cases mirror the ones in test_console_log_capture.py
@freeze_time("2025-04-23 00:00:00")
@pytest.mark.parametrize(
    "messages, expected",
    [
        (["Hello"], ["Hello"]),
        (["Hello", "World"], ["Hello", "World"]),
        (["Hello\nWorld"], ["Hello", "World"]),
        (["Hello\rWorld"], ["World"]),
        (["Hello\n"], ["Hello"]),
        (["\nHello"], ["Hello"]),
        (["Hello\r"], []),
        (["\rHello"], ["Hello"]),
        (["Hello\nWorld\n"], ["Hello", "World"]),
        (["Hello\rWorld\r"], []),
        (["Hello\rWorld\rNow"], ["Now"]),
        (["Hello\r\nWorld\r\n"], []),
        (["Hello\n\rWorld\n\r"], ["Hello", "World"]),
        (["." * (2 * LINE_LIMIT + 500)], ["." * LINE_LIMIT, "." * LINE_LIMIT, "." * 500]),
        (["." * (2 * LINE_LIMIT) + "\rHello"], ["Hello"]),
        (["." * 1024 + "\n" + "." * (LINE_LIMIT + 500)], ["." * 1024, "." * LINE_LIMIT, "." * 500]),
        (["." * (LINE_LIMIT - 1) + "漢"], ["." * (LINE_LIMIT - 1), "漢"]),
    ],
)
def test_splitting_lines(messages, expected):
    logger = logging.getLogger("test_logger")
    logger.setLevel(logging.DEBUG)

    ts = datetime(2025, 4, 23, 0, 0)

    with Run(project="workspace/project", mode="offline") as run:
        run.log_string_series = Mock()

        logger.addHandler(NeptuneLoggingHandler(run=run, attribute_path="test/path"))

        for message in messages:
            logger.info(message)

        for i, expected_line in enumerate(expected):
            run.log_string_series.assert_any_call(data={"test/path": expected_line}, step=i + 1, timestamp=ts)


def test_max_path_length():
    with Run(project="workspace/project", mode="offline") as run:
        with pytest.raises(ValueError):
            NeptuneLoggingHandler(run=run, attribute_path="a" * (MAX_ATTRIBUTE_PATH_LENGTH + 1))
