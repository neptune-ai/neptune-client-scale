import io
import sys
import threading
from collections.abc import Generator
from datetime import (
    datetime,
    timedelta,
)
from typing import (
    Any,
    Callable,
    Optional,
    Union,
)

from neptune_scale.exceptions import NeptuneUnableToLogData
from neptune_scale.sync.parameters import MAX_STRING_SERIES_DATA_POINT_LENGTH
from neptune_scale.util import Daemon

STREAM_BUFFER_CHAR_CAPACITY = 100_000_000


class StreamWithMemory:
    def __init__(self, original_stream: io.TextIOBase):
        self._original_stream = original_stream
        self._fifo_buffer: list[tuple[datetime, str]] = []
        self._chars_in_buffer = 0
        self._lock = threading.RLock()
        self._subscribers_to_offset: dict[str, int] = {}

    def register_subscriber(self, subscriber_id: str) -> None:
        with self._lock:
            if subscriber_id not in self._subscribers_to_offset:
                self._subscribers_to_offset[subscriber_id] = len(self._fifo_buffer)

    def unregister_subscriber(self, subscriber_id: str) -> int:
        with self._lock:
            if subscriber_id in self._subscribers_to_offset:
                del self._subscribers_to_offset[subscriber_id]

            return len(self._subscribers_to_offset)

    def write(self, data: str) -> int:
        """
        Buffers the data with a timestamp for background processing AND
        writes the data immediately to the original stream.
        """
        ts = datetime.now()
        chars_written = self._original_stream.write(data)
        with self._lock:
            space_left_in_buffer = STREAM_BUFFER_CHAR_CAPACITY - self._chars_in_buffer
            if chars_written > space_left_in_buffer:
                # If the buffer is full, drop the oldest data
                # We always want to drop at least half of the data so that the amortized complexity remains O(1)
                chars_to_drop = max(chars_written, STREAM_BUFFER_CHAR_CAPACITY // 2)
                self._drop_first_chars_from_buffer(chars_to_drop)

            chars_to_save_in_buffer = min(chars_written, STREAM_BUFFER_CHAR_CAPACITY)
            self._fifo_buffer.append((ts, data[:chars_to_save_in_buffer]))
            self._chars_in_buffer += chars_to_save_in_buffer

        return chars_written

    def get_buffered_data(self, subscriber_id: str) -> list[tuple[datetime, str]]:
        """
        Atomically retrieves all buffered data for a specific subscriber (from its current offset till the end).
        """
        with self._lock:
            offset = self._subscribers_to_offset[subscriber_id]
            data_list = self._fifo_buffer[offset:]
            self._subscribers_to_offset[subscriber_id] = len(self._fifo_buffer)
            return data_list

    def _drop_first_chars_from_buffer(self, chars_to_drop: int) -> None:
        """
        Drops the first n entries from the buffer and adjust subscriber offsets accordingly.
        """
        with self._lock:
            chars_to_drop = min(chars_to_drop, self._chars_in_buffer)
            prefix_chars_count = 0
            entries_to_drop = 0
            while prefix_chars_count < chars_to_drop and entries_to_drop < len(self._fifo_buffer):
                prefix_chars_count += len(self._fifo_buffer[entries_to_drop][1])
                entries_to_drop += 1

            self._fifo_buffer = self._fifo_buffer[entries_to_drop:]
            self._chars_in_buffer -= prefix_chars_count

            for subscriber_id, offset in self._subscribers_to_offset.items():
                if offset >= entries_to_drop:
                    self._subscribers_to_offset[subscriber_id] = offset - entries_to_drop
                else:
                    self._subscribers_to_offset[subscriber_id] = 0

    def __getattr__(self, attr: str) -> Any:
        return getattr(self._original_stream, attr)


_lock = threading.RLock()
_stdout_with_memory: Optional[StreamWithMemory] = None
_stderr_with_memory: Optional[StreamWithMemory] = None
_original_stdout = sys.stdout
_original_stderr = sys.stderr


def _subscribe(subscriber_id: str) -> None:
    global _stdout_with_memory, _stderr_with_memory, _lock
    with _lock:
        if _stdout_with_memory is None:
            _stdout_with_memory = StreamWithMemory(sys.stdout)  # type: ignore
            sys.stdout = _stdout_with_memory
        if _stderr_with_memory is None:
            _stderr_with_memory = StreamWithMemory(sys.stderr)  # type: ignore
            sys.stderr = _stderr_with_memory

        _stdout_with_memory.register_subscriber(subscriber_id)
        _stderr_with_memory.register_subscriber(subscriber_id)


def _cancel_subscription(subscriber_id: str) -> None:
    global _stdout_with_memory, _stderr_with_memory, _lock
    with _lock:
        if _stdout_with_memory is not None:
            remaining_subscribers = _stdout_with_memory.unregister_subscriber(subscriber_id)
            if remaining_subscribers == 0:
                sys.stdout = _original_stdout
                _stdout_with_memory = None

        if _stderr_with_memory is not None:
            remaining_subscribers = _stderr_with_memory.unregister_subscriber(subscriber_id)
            if remaining_subscribers == 0:
                sys.stderr = _original_stderr
                _stderr_with_memory = None


class PartialLine:
    def __init__(self) -> None:
        self.buffer: io.StringIO = io.StringIO()
        self.timestamp: Optional[datetime] = None

    def write(self, ts: datetime, data: str) -> None:
        self.buffer.write(data)
        self.timestamp = ts

    def clear(self) -> None:
        self.buffer.truncate(0)
        self.buffer.seek(0)
        self.timestamp = None


class MutableInt:
    def __init__(self, value: int) -> None:
        self.value = value


class ConsoleLogCaptureThread(Daemon):
    STDOUT_ATTRIBUTE = "monitoring/stdout"
    STDERR_ATTRIBUTE = "monitoring/stderr"

    def __init__(
        self,
        run_id: str,
        logs_flush_frequency_sec: float,
        logs_sink: Callable[[dict[str, str], Union[float, int], Optional[datetime]], None],
    ) -> None:
        super().__init__(logs_flush_frequency_sec, "ConsoleLogCapture")

        self._logs_sink = logs_sink
        self._run_id = run_id

        _subscribe(run_id)

        self._stdout_partial_line = PartialLine()
        self._stdout_step = MutableInt(1)

        self._stderr_partial_line = PartialLine()
        self._stderr_step = MutableInt(1)

    def work(self) -> None:
        self._process_captured_data(max_delay_before_flush=timedelta(seconds=1))

    def close(self) -> None:
        self._process_captured_data(max_delay_before_flush=timedelta(seconds=0))
        _cancel_subscription(self.name)

    def _process_captured_data(self, max_delay_before_flush: timedelta) -> None:
        try:
            assert _stdout_with_memory is not None
            assert _stderr_with_memory is not None

            self._process_captured_data_single_stream(
                self.STDOUT_ATTRIBUTE,
                self._stdout_partial_line,
                self._stdout_step,
                _stdout_with_memory.get_buffered_data(self._run_id),
                max_delay_before_flush,
            )

            self._process_captured_data_single_stream(
                self.STDERR_ATTRIBUTE,
                self._stderr_partial_line,
                self._stderr_step,
                _stderr_with_memory.get_buffered_data(self._run_id),
                max_delay_before_flush,
            )

        except NeptuneUnableToLogData as e:
            _print_to_original_stderr(f"Unable to log data from stdout/stderr: {e}")

        except Exception as e:
            _cancel_subscription(self.name)
            raise e

    def _process_captured_data_single_stream(
        self,
        attribute_name: str,
        partial_line: PartialLine,
        step: MutableInt,
        data: list[tuple[datetime, str]],
        max_delay_before_flush: timedelta,
    ) -> None:
        for ts, line in _captured_data_to_lines(partial_line, data, max_delay_before_flush):
            for short_line in _split_long_line(line, MAX_STRING_SERIES_DATA_POINT_LENGTH):
                self._logs_sink({attribute_name: short_line}, step.value, ts)
                step.value += 1


def _captured_data_to_lines(
    partial_line: PartialLine,
    data: list[tuple[datetime, str]],
    max_delay_before_flush: timedelta,
) -> Generator[tuple[datetime, str]]:
    """
    Splits the captured data into lines.
    If a carriage return (\r) char is found, the line is reset.

    Each line is yielded with a timestamp from its latest chunk.
    If flush_partial_line is True, the last partial line is yielded as well.
    """
    for timestamp, chunk in data:
        if not chunk:
            continue

        chunk_processed_idx = 0
        while (lf_pos := chunk.find("\n", chunk_processed_idx)) != -1:
            cr_pos = chunk.rfind("\r", chunk_processed_idx, lf_pos)
            if cr_pos != -1:
                partial_line.clear()
                chunk_processed_idx = cr_pos + 1  # skip over CR

            partial_line.write(timestamp, chunk[chunk_processed_idx:lf_pos])

            yield timestamp, partial_line.buffer.getvalue()
            partial_line.clear()

            chunk_processed_idx = lf_pos + 1  # skip over LF

        if chunk_processed_idx < len(chunk):
            cr_pos = chunk.rfind("\r", chunk_processed_idx)
            if cr_pos != -1:
                partial_line.clear()
                chunk_processed_idx = cr_pos + 1  # skip over CR

            partial_line.write(timestamp, chunk[chunk_processed_idx:])

    if partial_line.timestamp and datetime.now() - partial_line.timestamp > max_delay_before_flush:
        if line := partial_line.buffer.getvalue():
            yield partial_line.timestamp, line
            partial_line.clear()


def _split_long_line(line: str, max_bytes: int) -> list[str]:
    """
    Splits a potentially long string into multiple chunks, ensuring no chunk
    exceeds max_bytes when encoded as UTF-8 and never splits a multibyte character.
    """
    try:
        # Encode the entire line to bytes to measure accurately
        line_bytes = line.encode("utf-8", errors="replace")
        total_bytes = len(line_bytes)

        # If the line is already within the limit, return it as a single chunk
        if total_bytes <= max_bytes:
            return [line]

        chunks = []
        start_byte = 0

        while start_byte < total_bytes:
            if start_byte + max_bytes >= total_bytes:
                # Last chunk, just include everything remaining
                end_byte = total_bytes
            else:
                # Find the largest valid end position that doesn't exceed max_bytes
                end_byte = start_byte + max_bytes

                # Back up until we're at a valid UTF-8 character boundary
                # Valid boundary - byte is not a continuation byte (10xxxxxx)
                while end_byte > start_byte and (line_bytes[end_byte] & 0xC0) == 0x80:
                    end_byte -= 1

            # Get the byte slice for the current chunk
            chunk_bytes = line_bytes[start_byte:end_byte]
            # Decode the byte chunk back into a string
            chunks.append(chunk_bytes.decode("utf-8", errors="replace"))
            # Move the start index for the next chunk
            start_byte = end_byte

        return chunks
    except Exception as e:
        _print_to_original_stderr(f"Error in stdout/stderr capture: {e}")
        return []


def _print_to_original_stderr(message: str) -> None:
    original_stderr = getattr(sys, "__stderr__", sys.stderr)
    original_stderr.write(message)
    original_stderr.flush()
