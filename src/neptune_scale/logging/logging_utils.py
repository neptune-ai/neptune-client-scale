import io
import sys
from collections.abc import Generator
from datetime import (
    datetime,
    timedelta,
)
from typing import Optional


class PartialLine:
    def __init__(self) -> None:
        self.buffer: io.StringIO = io.StringIO()
        self.timestamp: Optional[datetime] = None
        self.last_flush_time: Optional[datetime] = None

    def write(self, ts: datetime, data: str) -> None:
        self.buffer.write(data)
        self.timestamp = ts

    def clear(self) -> None:
        self.buffer.truncate(0)
        self.buffer.seek(0)
        self.timestamp = None

    def flush(self) -> tuple[Optional[datetime], str]:
        data = self.buffer.getvalue()
        ts = self.timestamp
        self.clear()
        self.last_flush_time = datetime.now()
        return ts, data


def captured_data_to_lines(
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
            ts, line = partial_line.flush()
            if ts and line:
                yield ts, line

            chunk_processed_idx = lf_pos + 1  # skip over LF

        if chunk_processed_idx < len(chunk):
            cr_pos = chunk.rfind("\r", chunk_processed_idx)
            if cr_pos != -1:
                partial_line.clear()
                chunk_processed_idx = cr_pos + 1  # skip over CR

            partial_line.write(timestamp, chunk[chunk_processed_idx:])

    if not partial_line.last_flush_time or datetime.now() - partial_line.last_flush_time >= max_delay_before_flush:
        ts, line = partial_line.flush()
        if ts and line:
            yield ts, line


def split_long_line(line: str, max_bytes: int) -> list[str]:
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
        print_to_original_stderr(f"Error in stdout/stderr capture: {e}")
        return []


def print_to_original_stderr(message: str) -> None:
    original_stderr = getattr(sys, "__stderr__", sys.stderr)
    original_stderr.write(message + "\n")
    original_stderr.flush()
