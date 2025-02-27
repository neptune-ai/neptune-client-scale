from __future__ import annotations

__all__ = ("OperationsRepository", "OperationType", "Operation", "Metadata")

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
    metadata_size: int


@dataclass(frozen=True)
class Metadata:
    version: str
    project: str
    run_id: str
    parent: Optional[str] = None
    fork_step: Optional[float] = None


class OperationsRepository:
    """A disk-based repository for Neptune operations using SQLite.

    This repository stores and manages Neptune operations in a SQLite database with the following schema:
    - Table `run_operations`: Contains serialized RunOperation objects
      - sequence_id INTEGER: Auto-incremented operation ID (Primary Key)
      - timestamp INTEGER: Timestamp of the operation being enqueued
      - operation_type INTEGER NOT NULL: Type of operation (0=snapshot, 1=create_run)
      - operation BLOB NOT NULL: Serialized RunOperation object (protobuf)
      - metadata_size INTEGER NOT NULL: Size of the metadata in the operation

    - Table `metadata`: Contains metadata about runs
      - id INTEGER: Auto-incremented ID (Primary Key)
      - version TEXT: Version identifier
      - project TEXT: Project identifier
      - run_id TEXT: Run identifier
      - parent TEXT: Parent run identifier (optional)
      - fork_step REAL: Fork step (optional)
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._lock = threading.RLock()
        self._connection: Optional[sqlite3.Connection] = None

        self._init_db()

    def _init_db(self) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(self._db_path)), exist_ok=True)
        with self._get_connection() as conn:  # type: ignore
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS run_operations (
                    sequence_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER NOT NULL,
                    operation_type INTEGER NOT NULL,
                    operation BLOB NOT NULL,
                    metadata_size INTEGER NOT NULL
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
                    parent TEXT,
                    fork_step REAL
                )"""
            )

    def save_update_run_snapshots(self, ops: list[UpdateRunSnapshot]) -> SequenceId:
        current_time = int(time.time() * 1000)  # milliseconds timestamp
        params = []

        for update in ops:
            serialized_operation = update.SerializeToString()
            metadata_size = len(serialized_operation)
            params.append((current_time, OperationType.UPDATE_SNAPSHOT, serialized_operation, metadata_size))

        with self._get_connection() as conn:  # type: ignore
            cursor = conn.cursor()

            cursor.executemany(
                "INSERT INTO run_operations (timestamp, operation_type, operation, metadata_size) VALUES (?, ?, ?, ?)",
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
            metadata_size = len(serialized_operation)

            cursor.execute(
                "INSERT INTO run_operations (timestamp, operation_type, operation, metadata_size) VALUES (?, ?, ?, ?)",
                (current_time, OperationType.CREATE_RUN, serialized_operation, metadata_size),
            )
            return SequenceId(cursor.lastrowid)  # type: ignore

    def get_operations(self, up_to_bytes: int, window_function_limit: int = 10_000) -> list[Operation]:
        with self._get_connection() as conn:  # type: ignore
            cursor = conn.cursor()
            cursor.execute(
                """
                WITH running_size AS (
                    SELECT
                        sequence_id,
                        timestamp,
                        operation,
                        operation_type,
                        metadata_size,
                        SUM(metadata_size) OVER (ORDER BY sequence_id ASC) AS cumulative_size
                    FROM run_operations
                    ORDER BY sequence_id ASC
                    LIMIT ?
                )
                SELECT sequence_id, timestamp, operation, operation_type, metadata_size
                FROM running_size
                WHERE cumulative_size <= ?
                ORDER BY sequence_id ASC
                """,
                (
                    window_function_limit,
                    up_to_bytes,
                ),
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
        self, project: str, run_id: str, parent: Optional[str] = None, fork_step: Optional[float] = None
    ) -> None:
        # TODO maybe should be called with save_create_run
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
                INSERT INTO metadata (version, project, run_id, parent, fork_step)
                VALUES (?, ?, ?, ?, ?)
                """,
                (DB_VERSION, project, run_id, parent, fork_step),
            )

    def get_metadata(self) -> Optional[Metadata]:
        with self._get_connection() as conn:  # type: ignore
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT version, project, run_id, parent, fork_step
                FROM metadata
                """
            )

            row = cursor.fetchone()
            if not row:
                return None

            version, project, run_id, parent, fork_step = row

            return Metadata(
                version=version,
                project=project,
                run_id=run_id,
                parent=parent,
                fork_step=fork_step
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

            with self._connection:
                yield self._connection


def _deserialize_operation(row: tuple[int, int, bytes, int, int]) -> Operation:
    sequence_id, timestamp, operation, operation_type, metadata_size = row
    op_type = OperationType(operation_type)

    deserialized_op = (
        UpdateRunSnapshot.FromString(operation)
        if op_type == OperationType.UPDATE_SNAPSHOT
        else CreateRun.FromString(operation)
    )
    return Operation(SequenceId(sequence_id), timestamp, op_type, deserialized_op, metadata_size)
