# ruff: noqa: T201
import importlib
import sys
import time
from unittest.mock import (
    ANY,
    Mock,
)

import pytest

import neptune_scale.logging.console_log_capture
from neptune_scale.logging.console_log_capture import (
    ConsoleLogCaptureThread,
    StepTracker,
    StreamWithMemory,
)


def test_stream_with_memory_passes_data_to_original_stream():
    # given
    original_stream = Mock()
    original_stream.write = Mock(side_effect=lambda x: len(x))
    stream = StreamWithMemory(original_stream)

    # when
    stream.write("Hello")
    stream.write("World")

    # then
    original_stream.write.assert_any_call("Hello")
    original_stream.write.assert_any_call("World")


def test_stream_with_memory_passes_data_to_subscriber():
    # given
    original_stream = Mock()
    original_stream.write = Mock(side_effect=lambda x: len(x))
    stream = StreamWithMemory(original_stream)
    subscriber_id = "subscriber_id"

    # when
    stream.register_subscriber(subscriber_id)
    stream.write("Hello")
    stream.write("World")

    # then
    data = stream.get_buffered_data(subscriber_id)
    assert [line for _, line in data] == ["Hello", "World"]


def test_stream_with_memory_passes_data_to_subscriber_with_multiple_subscribers():
    # given
    original_stream = Mock()
    original_stream.write = Mock(side_effect=lambda x: len(x))
    stream = StreamWithMemory(original_stream)
    subscriber_id_1 = "subscriber_id1"
    subscriber_id_2 = "subscriber_id2"

    # when
    stream.register_subscriber(subscriber_id_1)
    stream.register_subscriber(subscriber_id_2)
    stream.write("Hello")
    stream.write("World")

    # then
    data_1 = stream.get_buffered_data(subscriber_id_1)
    data_2 = stream.get_buffered_data(subscriber_id_2)

    assert [line for _, line in data_1] == ["Hello", "World"]
    assert [line for _, line in data_2] == ["Hello", "World"]


@pytest.mark.parametrize(
    "initial, increment_count, expected",
    (
        (0, 0, 0),
        (1, 0, 1),
        (1.000001, 0, 1.000001),
        (0.123456, 0, 0.123456),
        (0.9999999999, 0, 0.999999),
        (0.999999, 0, 0.999999),
        (1.123456789, 0, 1.123456),
        (1.000001, 1, 1.000002),
        (0.999999, 1, 1.000000),
        (1, 1, 1.000001),
        (1.123456888, 1_000_000, 2.123456),
        (1, 2_500_001, 3.500001),
    ),
)
def test_step_tracker(initial, increment_count, expected):
    tracker = StepTracker(initial)

    last_value = tracker.value
    for _ in range(increment_count):
        last_value = tracker.increment()

    assert last_value == expected
    assert tracker.value == expected


def test_stream_with_memory_passes_data_to_subscriber_with_multiple_writes():
    # given
    original_stream = Mock()
    original_stream.write = Mock(side_effect=lambda x: len(x))
    stream = StreamWithMemory(original_stream)
    subscriber_id = "subscriber_id"

    # when
    stream.register_subscriber(subscriber_id)
    stream.write("Hello")
    stream.write("World")

    # then
    data = stream.get_buffered_data(subscriber_id)
    assert [line for _, line in data] == ["Hello", "World"]

    # when
    stream.write("Hello 2")
    stream.write("World 2")

    # then
    data = stream.get_buffered_data(subscriber_id)
    assert [line for _, line in data] == ["Hello 2", "World 2"]


def test_stream_with_memory_passes_data_to_subscriber_until_unsubscribe():
    # given
    original_stream = Mock()
    original_stream.write = Mock(side_effect=lambda x: len(x))
    stream = StreamWithMemory(original_stream)
    subscriber_id = "subscriber_id"

    # when
    stream.register_subscriber(subscriber_id)
    stream.write("Hello")
    stream.write("World")

    # then
    data = stream.get_buffered_data(subscriber_id)
    assert [line for _, line in data] == ["Hello", "World"]

    # when
    stream.unregister_subscriber(subscriber_id)
    stream.write("Hello 2")
    stream.write("World 2")

    # then
    with pytest.raises(KeyError):
        stream.get_buffered_data(subscriber_id)


@pytest.fixture
def no_capture(capsys):
    with capsys.disabled():
        # reload the module so that it reassigns sys.stdout/stderr to the streams set by capsys
        importlib.reload(neptune_scale.logging.console_log_capture)
        yield


def test_console_log_capture_thread_captures_stdout(no_capture):
    # given
    logs_sink = Mock()
    thread = ConsoleLogCaptureThread(
        run_id="run_id", system_namespace="system", initial_step=0, logs_flush_frequency_sec=0.1, logs_sink=logs_sink
    )

    # when
    thread.start()
    print("Hello")
    print("World")
    thread.interrupt(remaining_iterations=1)
    thread.join()

    # then
    logs_sink.assert_any_call({"system/stdout": "Hello"}, _make_step(1), ANY)
    logs_sink.assert_any_call({"system/stdout": "World"}, _make_step(2), ANY)


def test_console_log_capture_thread_captures_stderr(no_capture):
    # given
    logs_sink = Mock()
    thread = ConsoleLogCaptureThread(
        run_id="run_id", system_namespace="system", initial_step=0, logs_flush_frequency_sec=0.1, logs_sink=logs_sink
    )

    # when
    thread.start()
    print("Hello", file=sys.stderr)
    print("World", file=sys.stderr)
    thread.interrupt(remaining_iterations=1)
    thread.join()

    # then
    logs_sink.assert_any_call({"system/stderr": "Hello"}, _make_step(1), ANY)
    logs_sink.assert_any_call({"system/stderr": "World"}, _make_step(2), ANY)


def test_console_log_capture_thread_captures_both_stdout_and_stderr(no_capture):
    # given
    logs_sink = Mock()
    thread = ConsoleLogCaptureThread(
        run_id="run_id", system_namespace="system", initial_step=0, logs_flush_frequency_sec=0.1, logs_sink=logs_sink
    )

    # when
    thread.start()
    print("Hello stdout")
    print("Hello stderr", file=sys.stderr)
    print("World stdout")
    print("World stderr", file=sys.stderr)
    thread.interrupt(remaining_iterations=1)
    thread.join()

    # then
    logs_sink.assert_any_call({"system/stdout": "Hello stdout"}, _make_step(1), ANY)
    logs_sink.assert_any_call({"system/stderr": "Hello stderr"}, _make_step(1), ANY)
    logs_sink.assert_any_call({"system/stdout": "World stdout"}, _make_step(2), ANY)
    logs_sink.assert_any_call({"system/stderr": "World stderr"}, _make_step(2), ANY)


LINE_LIMIT = 1024 * 1024


def _make_step(index: int, initial: float = 0) -> float:
    # Return a step value that can be compared against actual calls made,
    return StepTracker(initial + round(index / 1e6, 6)).value


# The test cases mirror the ones in test_neptune_logging_handler.py
@pytest.mark.parametrize(
    "prints, expected",
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
def test_console_log_capture_thread_split_lines(no_capture, prints, expected):
    # given
    logs_sink = Mock()
    thread = ConsoleLogCaptureThread(
        run_id="run_id", initial_step=0, system_namespace="system", logs_flush_frequency_sec=10, logs_sink=logs_sink
    )

    # when
    thread.start()
    for line in prints:
        print(line)
    thread.interrupt(remaining_iterations=1)
    thread.join()

    # then
    if expected:
        for idx, line in enumerate(expected):
            logs_sink.assert_any_call({"system/stdout": line}, _make_step(idx + 1), ANY)
    else:
        logs_sink.assert_not_called()


@pytest.mark.parametrize(
    "prints, expected",
    [
        (["Hello"], ["Hello"]),
        (["Hello", "World"], ["HelloWorld"]),
        (["."] * 10, ["." * 10]),
    ],
)
def test_console_log_capture_thread_merge_lines(no_capture, prints, expected):
    # given
    logs_sink = Mock()
    thread = ConsoleLogCaptureThread(
        run_id="run_id", system_namespace="system", initial_step=0, logs_flush_frequency_sec=2, logs_sink=logs_sink
    )

    # when
    thread.start()
    for line in prints:
        print(line, end="")

    time.sleep(4)
    thread.interrupt(remaining_iterations=1)
    thread.join()

    # then
    for idx, line in enumerate(expected):
        logs_sink.assert_any_call({"system/stdout": line}, _make_step(idx + 1), ANY)


@pytest.mark.parametrize("initial_step", (0, 1.0, 1.1, 1.12, 1.123, 1.1234, 1.12345, 1.123456))
def test_console_log_capture_thread_initial_step(no_capture, initial_step):
    # given
    logs_sink = Mock()
    thread = ConsoleLogCaptureThread(
        run_id="run_id",
        initial_step=initial_step,
        system_namespace="system",
        logs_flush_frequency_sec=0.1,
        logs_sink=logs_sink,
    )

    # when
    thread.start()
    for line in range(10):
        print(f"line-{line}")

    thread.interrupt(remaining_iterations=1)
    thread.join()

    # then
    for idx in range(10):
        logs_sink.assert_any_call({"system/stdout": f"line-{idx}"}, _make_step(idx + 1, initial=initial_step), ANY)


def test_system_namespace(no_capture):
    # given
    logs_sink = Mock()
    thread = ConsoleLogCaptureThread(
        run_id="run_id",
        system_namespace="custom/namespace",
        initial_step=0,
        logs_flush_frequency_sec=0.1,
        logs_sink=logs_sink,
    )

    # when
    thread.start()
    print("Hello")
    print("World")
    thread.interrupt(remaining_iterations=1)
    thread.join()

    # then
    logs_sink.assert_any_call({"custom/namespace/stdout": "Hello"}, _make_step(1), ANY)
    logs_sink.assert_any_call({"custom/namespace/stdout": "World"}, _make_step(2), ANY)
