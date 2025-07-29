import io
import sys
import threading
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
from neptune_scale.logging.logging_utils import (
    PartialLine,
    captured_data_to_lines,
    print_to_original_stderr,
    split_long_line,
)
from neptune_scale.sync.metadata_splitter import decompose_step
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

        # Handle case where write() returns None (some stream implementations do this)
        if chars_written is None:
            chars_written = len(data)

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


class StepTracker:
    def __init__(self, initial_step: Union[int, float]) -> None:
        """Track a float step value with a precision of 1e-6."""
        self.whole, self.micro = decompose_step(initial_step)
        # Always start with the subsequent step
        self.increment()

    @property
    def value(self) -> float:
        return self._value

    def increment(self) -> float:
        """Increment the step and return the new value."""
        self.micro += 1
        if self.micro >= 1_000_000:
            self.micro = 0
            self.whole += 1

        self._value = self.whole + self.micro / 1_000_000
        return self._value


class ConsoleLogCaptureThread(Daemon):
    def __init__(
        self,
        *,
        run_id: str,
        runtime_namespace: str,
        initial_step: Union[int, float],
        logs_flush_frequency_sec: float,
        logs_sink: Callable[[dict[str, str], Union[float, int], Optional[datetime]], None],
    ) -> None:
        super().__init__(logs_flush_frequency_sec, "ConsoleLogCapture")

        self._logs_sink = logs_sink
        self._run_id = run_id

        _subscribe(run_id)

        self._stdout_partial_line = PartialLine()
        self._stdout_attribute = f"{runtime_namespace}/stdout"
        self._stdout_step = StepTracker(initial_step)

        self._stderr_partial_line = PartialLine()
        self._stderr_attribute = f"{runtime_namespace}/stderr"
        self._stderr_step = StepTracker(initial_step)

    def work(self) -> None:
        self._process_captured_data(max_delay_before_flush=timedelta(seconds=5))

    def close(self) -> None:
        self._process_captured_data(max_delay_before_flush=timedelta(seconds=0))
        _cancel_subscription(self.name)

    def _process_captured_data(self, max_delay_before_flush: timedelta) -> None:
        try:
            assert _stdout_with_memory is not None
            assert _stderr_with_memory is not None

            self._process_captured_data_single_stream(
                self._stdout_attribute,
                self._stdout_partial_line,
                self._stdout_step,
                _stdout_with_memory.get_buffered_data(self._run_id),
                max_delay_before_flush,
            )

            self._process_captured_data_single_stream(
                self._stderr_attribute,
                self._stderr_partial_line,
                self._stderr_step,
                _stderr_with_memory.get_buffered_data(self._run_id),
                max_delay_before_flush,
            )

        except NeptuneUnableToLogData as e:
            print_to_original_stderr(f"Unable to log data from stdout/stderr: {e}")

        except Exception as e:
            _cancel_subscription(self.name)
            raise e

    def _process_captured_data_single_stream(
        self,
        attribute_name: str,
        partial_line: PartialLine,
        step: StepTracker,
        data: list[tuple[datetime, str]],
        max_delay_before_flush: timedelta,
    ) -> None:
        for ts, line in captured_data_to_lines(partial_line, data, max_delay_before_flush):
            for short_line in split_long_line(line, MAX_STRING_SERIES_DATA_POINT_LENGTH):
                self._logs_sink({attribute_name: short_line}, step.value, ts)
                step.increment()
