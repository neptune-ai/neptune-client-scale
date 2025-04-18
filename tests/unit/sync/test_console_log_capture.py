from unittest.mock import Mock

from neptune_scale.sync.console_log_capture import (
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
