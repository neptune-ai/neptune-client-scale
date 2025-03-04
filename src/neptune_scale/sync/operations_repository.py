from __future__ import annotations

from pathlib import Path

__all__ = ("OperationsRepository", "OperationType", "Operation", "Metadata", "SequenceId")

import contextlib
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


@dataclass(frozen=True)
class Metadata:
    version: str
    project: str
    run_id: str
    parent_run_id: Optional[str] = None
    fork_step: Optional[float] = None


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
                    run_id TEXT NOT NULL,
                    parent_run_id TEXT,
                    fork_step REAL
                )"""
            )

    def save_update_run_snapshots(self, ops: list[UpdateRunSnapshot]) -> SequenceId:
        current_time = int(time.time() * 1000)  # milliseconds timestamp
        params = []

        for update in ops:
            serialized_operation = update.SerializeToString()
            operation_size_bytes = len(serialized_operation)
            params.append((current_time, OperationType.UPDATE_SNAPSHOT, serialized_operation, operation_size_bytes))

        with self._get_connection() as conn:  # type: ignore
            cursor = conn.cursor()

            cursor.executemany(
                "INSERT INTO run_operations (timestamp, operation_type, operation, operation_size_bytes) VALUES (?, ?, ?, ?)",
                params,
            )
            cursor.execute("SELECT last_insert_rowid()")
            last_insert_rowid: int = cursor.fetchone()[0]

            return SequenceId(last_insert_rowid)

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

    def get_operations(self, up_to_bytes: int) -> list[Operation]:
        with self._get_connection() as conn:  # type: ignore
            cursor = conn.cursor()

            def find_last_sequence_id_up_to_bytes() -> Optional[SequenceId]:
                limit = 50_000  # 2 * 8 bytes * 50_000 = 0.8MB
                _last_sequence_id = None
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
                        (_last_sequence_id or -1, limit),
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
                WHERE sequence_id <= ?
                ORDER BY sequence_id ASC
                """,
                (last_sequence_id,),
            )

            return [_deserialize_operation(row) for row in cursor.fetchall()]

    def delete_operations(self, up_to_seq_id: int) -> int:
        if up_to_seq_id <= 0:
            return 0

        with self._get_connection() as conn:  # type: ignore
            cursor = conn.cursor()

            cursor.execute("DELETE FROM run_operations WHERE sequence_id <= ?", (up_to_seq_id,))

            # Return the number of rows affected
            return cursor.rowcount or 0

    def save_metadata(
        self, project: str, run_id: str, parent_run_id: Optional[str] = None, fork_step: Optional[float] = None
    ) -> None:
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
                raise ValueError("Metadata already exists")

            # Insert new metadata
            cursor.execute(
                """
                INSERT INTO metadata (version, project, run_id, parent_run_id, fork_step)
                VALUES (?, ?, ?, ?, ?)
                """,
                (DB_VERSION, project, run_id, parent_run_id, fork_step),
            )

    def get_metadata(self) -> Optional[Metadata]:
        with self._get_connection() as conn:  # type: ignore
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT version, project, run_id, parent_run_id, fork_step
                FROM metadata
                """
            )

            row = cursor.fetchone()
            if not row:
                return None

            version, project, run_id, parent_run_id, fork_step = row

            return Metadata(
                version=version, project=project, run_id=run_id, parent_run_id=parent_run_id, fork_step=fork_step
            )

    def close(self) -> None:
        with self._lock:
            if self._connection is not None:
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
