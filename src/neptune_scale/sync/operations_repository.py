from __future__ import annotations

from pathlib import Path

from neptune_scale.sync.parameters import MAX_SINGLE_OPERATION_SIZE_BYTES

__all__ = ("OperationsRepository", "OperationType", "Operation", "Metadata", "SequenceId")

import contextlib
import datetime
import os
import sqlite3
import threading
import time
import typing
from contextlib import AbstractContextManager
from dataclasses import dataclass
from enum import IntEnum
from typing import (
    Optional,
    Union,
)

from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import Run as CreateRun
from neptune_api.proto.neptune_pb.ingest.v1.common_pb2 import UpdateRunSnapshot

from neptune_scale.exceptions import NeptuneLocalStorageInUnsupportedVersion
from neptune_scale.util import get_logger

logger = get_logger()

DB_VERSION = "v1"

SequenceId = typing.NewType("SequenceId", int)


class OperationType(IntEnum):
    UPDATE_SNAPSHOT = 0
    CREATE_RUN = 1


@dataclass(frozen=True)
class Operation:
    sequence_id: SequenceId
    timestamp: int
    operation_type: OperationType
    operation: Union[UpdateRunSnapshot, CreateRun]
    operation_size_bytes: int

    @property
    def ts(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(self.timestamp / 1000)


@dataclass(frozen=True)
class Metadata:
    project: str
    run_id: str


class OperationsRepository:
    """A disk-based repository for Neptune operations using SQLite.

    This repository stores and manages Neptune operations in a SQLite database with the following schema:
    - Table `run_operations`: Contains serialized RunOperation objects
      - sequence_id INTEGER: Auto-incremented operation ID (Primary Key)
      - timestamp INTEGER: Timestamp of the operation being enqueued
      - operation_type INTEGER NOT NULL: Type of operation (0=snapshot, 1=create_run)
      - operation BLOB NOT NULL: Serialized RunOperation object (protobuf)
      - operation_size_bytes INTEGER NOT NULL: Size of the operation

    - Table `metadata`: Contains metadata about runs
      - id INTEGER: Auto-incremented ID (Primary Key)
      - version TEXT: Version identifier
      - project TEXT: Project identifier
      - run_id TEXT: Run identifier
      - parent_run_id TEXT: Parent run identifier (optional)
      - fork_step REAL: Fork step (optional)
    """

    def __init__(self, db_path: Path) -> None:
        if not db_path.is_absolute():
            raise RuntimeError("db_path must be an absolute path")

        self._db_path = db_path
        self._lock = threading.RLock()
        self._connection: Optional[sqlite3.Connection] = None

    def init_db(self) -> None:
        os.makedirs(self._db_path.parent, exist_ok=True)
        with self._get_connection() as conn:  # type: ignore
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS run_operations (
                    sequence_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER NOT NULL,
                    operation_type INTEGER NOT NULL,
                    operation BLOB NOT NULL,
                    operation_size_bytes INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version TEXT NOT NULL,
                    project TEXT NOT NULL,
                    run_id TEXT NOT NULL
                )"""
            )

    def save_update_run_snapshots(self, updates: list[UpdateRunSnapshot]) -> SequenceId:
        """
        Guarantees:
        - The order of operations saved to the DB is preserved within a single call.
        - The timestamp is consistent for all operations in a single call.
        - When called by multiple threads, operations are not interleaved between threads.

        Returns -1 when the list of operations is empty.

        This method saves each chunk of operations in a separate transaction, so that
        a single large batch doesn’t lock the database for an extended period of time.
        """

        if not updates:
            return SequenceId(-1)

        # Chunk the operations so that no batch exceeds MAX_SINGLE_OPERATION_SIZE_BYTES.
        chunked_ops = self._chunk_operations(updates)

        last_insert_rowid = -1
        with self._lock:
            current_time = int(time.time() * 1000)  # millisecond timestamp
            for chunk in chunked_ops:
                last_insert_rowid = self._insert_update_run_snapshots(chunk, current_time)

        return SequenceId(last_insert_rowid)

    def _chunk_operations(self, updates: list[UpdateRunSnapshot]) -> list[list[bytes]]:
        """
        Split operations into batches so that each batch does not exceed MAX_SINGLE_OPERATION_SIZE_BYTES.
        Returns serialized operations in a list of batches.
        """
        batches: list[list[bytes]] = []
        current_batch: list[bytes] = []
        current_batch_size = 0

        for update in updates:
            serialized = update.SerializeToString()
            size_bytes = len(serialized)

            if size_bytes > MAX_SINGLE_OPERATION_SIZE_BYTES:
                raise RuntimeError(
                    f"Operation size ({size_bytes}) exceeds the limit of " f"{MAX_SINGLE_OPERATION_SIZE_BYTES} bytes"
                )

            # If adding this operation would exceed the limit, start a new batch.
            if current_batch_size + size_bytes > MAX_SINGLE_OPERATION_SIZE_BYTES:
                batches.append(current_batch)
                current_batch = []
                current_batch_size = 0

            current_batch.append(serialized)
            current_batch_size += size_bytes

        # Add the last batch if it's not empty
        if current_batch:
            batches.append(current_batch)

        return batches

    def _insert_update_run_snapshots(self, ops: list[bytes], current_time: int) -> SequenceId:
        """
        Inserts operations into 'run_operations' in a single transaction.
        """
        with self._get_connection() as conn:  # type: ignore
            cursor = conn.cursor()
            cursor.executemany(
                """
                INSERT INTO run_operations (timestamp, operation_type, operation, operation_size_bytes)
                VALUES (?, ?, ?, ?)
                """,
                [
                    (current_time, OperationType.UPDATE_SNAPSHOT, serialized_op, len(serialized_op))
                    for serialized_op in ops
                ],
            )
            cursor.execute("SELECT last_insert_rowid()")
            return SequenceId(cursor.fetchone()[0])

    def save_create_run(self, run: CreateRun) -> SequenceId:
        with self._get_connection() as conn:  # type: ignore
            cursor = conn.cursor()

            current_time = int(time.time() * 1000)  # milliseconds timestamp
            serialized_operation = run.SerializeToString()
            operation_size_bytes = len(serialized_operation)

            cursor.execute(
                "INSERT INTO run_operations (timestamp, operation_type, operation, operation_size_bytes) VALUES (?, ?, ?, ?)",
                (current_time, OperationType.CREATE_RUN, serialized_operation, operation_size_bytes),
            )
            return SequenceId(cursor.lastrowid)  # type: ignore

    def get_operations(self, up_to_bytes: int, from_exclusive: Optional[SequenceId] = None) -> list[Operation]:
        if up_to_bytes < MAX_SINGLE_OPERATION_SIZE_BYTES:
            raise RuntimeError(
                f"up to bytes is too small: {up_to_bytes} bytes, minimum is {MAX_SINGLE_OPERATION_SIZE_BYTES} bytes"
            )

        if from_exclusive is None:
            from_exclusive = SequenceId(-1)

        with self._get_connection() as conn:  # type: ignore
            cursor = conn.cursor()

            def find_last_sequence_id_up_to_bytes() -> Optional[SequenceId]:
                limit = 50_000  # 2 * 8 bytes * 50_000 = 0.8MB
                _last_sequence_id = from_exclusive
                total_operations_size_bytes = 0

                while True:
                    cursor.execute(
                        """
                        SELECT sequence_id, operation_size_bytes
                        FROM run_operations
                        WHERE sequence_id > ?
                        ORDER BY sequence_id ASC
                        LIMIT ?
                    """,
                        (_last_sequence_id, limit),
                    )
                    rows = cursor.fetchall()
                    if not rows:
                        return _last_sequence_id
                    for sequence_id, operation_size_bytes in rows:
                        if (total_operations_size_bytes + operation_size_bytes) > up_to_bytes:
                            return _last_sequence_id
                        _last_sequence_id = SequenceId(sequence_id)
                        total_operations_size_bytes += operation_size_bytes

                    # If we have less than limit rows, we can return the last sequence id
                    if len(rows) < limit:
                        return _last_sequence_id

            last_sequence_id = find_last_sequence_id_up_to_bytes()
            if last_sequence_id is None:
                return []
            cursor.execute(
                """
                SELECT sequence_id, timestamp, operation, operation_type, operation_size_bytes
                FROM run_operations
                WHERE sequence_id > ? AND sequence_id <= ?
                ORDER BY sequence_id ASC
                """,
                (
                    from_exclusive,
                    last_sequence_id,
                ),
            )

            return [_deserialize_operation(row) for row in cursor.fetchall()]

    def delete_operations(self, up_to_seq_id: SequenceId) -> int:
        if up_to_seq_id <= 0:
            return 0

        with self._get_connection() as conn:  # type: ignore
            cursor = conn.cursor()

            cursor.execute("DELETE FROM run_operations WHERE sequence_id <= ?", (up_to_seq_id,))

            # Return the number of rows affected
            return cursor.rowcount or 0

    def save_metadata(self, project: str, run_id: str) -> None:
        with self._get_connection() as conn:  # type: ignore
            cursor = conn.cursor()

            # Check if metadata already exists
            cursor.execute(
                """
                SELECT COUNT(*) FROM metadata
                """
            )

            count = cursor.fetchone()[0]
            if count > 0:
                raise RuntimeError("Metadata already exists")

            # Insert new metadata
            cursor.execute(
                """
                INSERT INTO metadata (version, project, run_id)
                VALUES (?, ?, ?)
                """,
                (DB_VERSION, project, run_id),
            )

    def get_metadata(self) -> Optional[Metadata]:
        with self._get_connection() as conn:  # type: ignore
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT version, project, run_id
                FROM metadata
                """
            )

            row = cursor.fetchone()
            if not row:
                return None

            version, project, run_id = row

            if version != DB_VERSION:
                raise NeptuneLocalStorageInUnsupportedVersion()

            return Metadata(project=project, run_id=run_id)

    def get_sequence_id_range(self) -> Optional[tuple[SequenceId, SequenceId]]:
        with self._get_connection() as conn:  # type: ignore
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT MIN(sequence_id), MAX(sequence_id)
                FROM run_operations
                """
            )

            row = cursor.fetchone()
            if not row:
                return None

            min_seq_id, max_seq_id = row
            if min_seq_id is None or max_seq_id is None:
                return None
            return SequenceId(min_seq_id), SequenceId(max_seq_id)

    def _is_run_operations_empty(self) -> bool:
        with self._get_connection() as conn:  # type: ignore
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='run_operations'")
            count: int = cursor.fetchone()[0]
            if count == 0:
                return True

            cursor.execute("SELECT COUNT(*) FROM run_operations")
            count = cursor.fetchone()[0]
            return count == 0

    def close(self, cleanup_files: bool) -> None:
        with self._lock:
            if self._connection is not None:
                if cleanup_files and self._is_run_operations_empty():
                    self._connection.close()
                    self._connection = None
                    try:
                        os.remove(self._db_path)
                    except OSError:
                        logger.debug(f"Failed to delete SQLite database file {self._db_path}", exc_info=True)
                        pass
                    logger.debug(f"Deleted SQLite database file {self._db_path}")
                else:
                    self._connection.close()
                    self._connection = None
                    logger.debug(f"Closed SQLite connection for {self._db_path}")

    @contextlib.contextmanager  # type: ignore
    def _get_connection(self) -> AbstractContextManager[sqlite3.Connection]:  # type: ignore
        with self._lock:
            if self._connection is None:
                self._connection = sqlite3.connect(
                    self._db_path,
                    check_same_thread=False,  # we use RLock to synchronize access
                )

                self._connection.execute("PRAGMA journal_mode = WAL")

                logger.debug(f"Created new SQLite connection for {self._db_path}")

            self._connection.execute("BEGIN")
            try:
                yield self._connection
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise


def _deserialize_operation(row: tuple[int, int, bytes, int, int]) -> Operation:
    sequence_id, timestamp, operation, operation_type, operation_size_bytes = row
    op_type = OperationType(operation_type)

    deserialized_op = (
        UpdateRunSnapshot.FromString(operation)
        if op_type == OperationType.UPDATE_SNAPSHOT
        else CreateRun.FromString(operation)
    )
    return Operation(SequenceId(sequence_id), timestamp, op_type, deserialized_op, operation_size_bytes)
