# ruff: noqa: T201

import sys
from unittest.mock import (
    ANY,
    Mock,
)

from neptune_scale.sync.console_log_capture import (
    ConsoleLogCaptureThread,
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


def test_console_log_capture_thread_captures_stdout():
    # given
    logs_sink = Mock()
    thread = ConsoleLogCaptureThread(run_id="run_id", logs_flush_frequency_sec=0.1, logs_sink=logs_sink)

    # when
    thread.start()
    print("Hello")
    print("World")
    thread.interrupt(remaining_iterations=1)
    thread.join()

    # then
    logs_sink.assert_any_call({"monitoring/stdout": "Hello"}, 1, ANY)
    logs_sink.assert_any_call({"monitoring/stdout": "World"}, 2, ANY)


def test_console_log_capture_thread_captures_stderr():
    # given
    logs_sink = Mock()
    thread = ConsoleLogCaptureThread(run_id="run_id", logs_flush_frequency_sec=0.1, logs_sink=logs_sink)

    # when
    thread.start()
    print("Hello", file=sys.stderr)
    print("World", file=sys.stderr)
    thread.interrupt(remaining_iterations=1)
    thread.join()

    # then
    logs_sink.assert_any_call({"monitoring/stderr": "Hello"}, 1, ANY)
    logs_sink.assert_any_call({"monitoring/stderr": "World"}, 2, ANY)


def test_console_log_capture_thread_captures_both_stdout_and_stderr():
    # given
    logs_sink = Mock()
    thread = ConsoleLogCaptureThread(run_id="run_id", logs_flush_frequency_sec=0.1, logs_sink=logs_sink)

    # when
    thread.start()
    print("Hello stdout")
    print("Hello stderr", file=sys.stderr)
    print("World stdout")
    print("World stderr", file=sys.stderr)
    thread.interrupt(remaining_iterations=1)
    thread.join()

    # then
    logs_sink.assert_any_call({"monitoring/stdout": "Hello stdout"}, 1, ANY)
    logs_sink.assert_any_call({"monitoring/stderr": "Hello stderr"}, 1, ANY)
    logs_sink.assert_any_call({"monitoring/stdout": "World stdout"}, 2, ANY)
    logs_sink.assert_any_call({"monitoring/stderr": "World stderr"}, 2, ANY)
