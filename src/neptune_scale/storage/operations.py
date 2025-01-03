import os
import sqlite3
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Optional,
    Union,
    cast,
)
from urllib.parse import quote

DATA_DIR = ".neptune"
BATCH_SIZE = 10000


class OperationWriter:
    def __init__(self, project: str, run_id: str, db_path: Optional[Path] = None, *, resume: bool = False) -> None:
        self._project = project
        self._run_id = run_id
        if db_path is None:
            db_path = database_path_for_run(project, run_id)
        self._db_path = db_path
        self._db: Optional[sqlite3.Connection] = None
        self._lock = threading.RLock()
        self._last_synced_op = 0
        self._resume = resume

    def write(self, serialized_op: bytes) -> None:
        """Store the given operation. It's assumed that `serialized_op` is a valid serialized RunOperation."""

        assert self._db  # mypy
        with self._lock, self._db:
            self._db.execute(
                """
                INSERT INTO operations (run_id, operation)
                VALUES (?, ?);""",
                (self._run_id, serialized_op),
            )

    def init_db(self) -> None:
        with self._lock:
            if self._resume and not os.path.isfile(self._db_path):
                raise FileNotFoundError(f"Neptune Database not found at {self._db_path}")

            # We use `check_same_thread=False` because closing the DB could be called from different
            # threads. It's fine, as long as we do our own synchronization.
            self._db = sqlite3.connect(self._db_path, autocommit=False, check_same_thread=False)
            if self._resume:
                self._last_synced_op = self._db.execute(
                    "SELECT last_synced_operation FROM meta WHERE run_id = ?", (self._run_id,)
                ).fetchone()[0]

    def mark_synced(self, sequence_id: int) -> None:
        """Mark an operation identified by the given `sequence_id` as synced with the Neptune backend."""

        assert self._db is not None  # mypy

        if sequence_id <= self._last_synced_op:
            raise ValueError(f"Already synced up to operation {self._last_synced_op}, got {sequence_id}")

        with self._lock, self._db:
            self._db.execute(
                """
                UPDATE meta
                SET last_synced_operation = ?
                WHERE run_id = ?""",
                (sequence_id, self._run_id),
            )

        self._last_synced_op = sequence_id

    def close(self) -> None:
        with self._lock:
            if self._db:
                self._db.close()
                self._db = None


@dataclass
class Operation:
    run_id: str
    seq: int
    data: bytes


class OperationReader:
    """
    This class is NOT thread-safe, however note that accessing a SQLite3 database from multiple threads is safe,
    as long as we use separate connection objects for each thread.

    Thus, it's fine if we have an OperationWriter instance in a different thread, updating the
    `meta` table will work as expected.
    """

    def __init__(self, db_path: Union[str, Path]) -> None:
        if not os.path.isfile(db_path):
            raise FileNotFoundError(f"Database not found at {db_path}")

        self._db = sqlite3.connect(db_path, autocommit=False, check_same_thread=False)
        self.project, self.run_id, self.last_synced_op = self._db.execute(
            "SELECT project, run_id, last_synced_operation FROM meta"
        ).fetchone()

        if self.project is None or self.run_id is None or self.last_synced_op is None:
            raise ValueError("Invalid Neptune database file")

        self.all_operations_count: int = self._db.execute(
            "SELECT count(*) from operations where run_id = ?", (self.run_id,)
        ).fetchone()[0]

    @property
    def pending_operations_count(self) -> int:
        return self.all_operations_count - cast(int, self.last_synced_op)

    @property
    def completed_operations_count(self) -> int:
        return cast(int, self.last_synced_op)

    @property
    def all_operations(self) -> Iterator[Operation]:
        yield from self._operations(0)

    @property
    def pending_operations(self) -> Iterator[Operation]:
        yield from self._operations(self.last_synced_op + 1)

    def _operations(self, start_seq: int) -> Iterator[Operation]:
        offset = 0
        while True:
            # Don't lock the database during the entire iteration. Pull data in batches.
            with self._db:
                batch = self._db.execute(
                    """
                    SELECT run_id, seq, operation FROM operations
                    WHERE run_id = ?
                    AND seq >= ?
                    ORDER BY seq ASC
                    LIMIT ? OFFSET ?""",
                    (self.run_id, start_seq, BATCH_SIZE, offset),
                ).fetchall()

            for run_id, seq, op in batch:
                yield Operation(run_id, seq, op)

            if len(batch) < BATCH_SIZE:
                break

            offset += len(batch)


def database_path_for_run(project: str, run_id: str, base_dir: Optional[str] = None) -> Path:
    return _data_dir(base_dir) / _safe_filename(project, run_id)


def init_write_storage(project: str, run_id: str, base_dir: Optional[str] = None) -> Path:
    """Initializes the local SQLite storage for writing operations. This function is called by Run in the
    main process, to give user a chance to react early to any storage-related errors.

    Returns:
        path to the SQLite store
    """

    data_dir = _data_dir(base_dir)
    os.makedirs(data_dir, exist_ok=True)

    path = database_path_for_run(project, run_id, base_dir)
    if path.exists():
        # TODO: validate if this is indeed an SQLite database; Check tables if they exist / add a migration hook.
        return path

    with sqlite3.connect(path) as conn:
        _init_db_schema(conn)
        _init_run(conn, project, run_id, resume=False)

    return path


def _init_db_schema(db: sqlite3.Connection) -> None:
    with db:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS operations (
                run_id TEXT NOT NULL,
                seq INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                operation BLOB NOT NULL,
                UNIQUE(run_id, seq)
            )"""
        )

        db.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
                version TEXT NOT NULL,
                project TEXT NOT NULL,
                run_id TEXT NOT NULL,
                last_synced_operation INTEGER,
                PRIMARY KEY (run_id),
                FOREIGN KEY (run_id, last_synced_operation) REFERENCES operations(run_id, seq)
            )"""
        )


def _init_run(db: sqlite3.Connection, project: str, run_id: str, *, resume: bool) -> None:
    with db:
        row = db.execute("SELECT 1 FROM meta WHERE run_id = ?;", (run_id,)).fetchone()
        if row is None:
            if resume:
                raise ValueError(f"Run {run_id} does not exist in local storage")
        else:
            return

        db.execute(
            """
            INSERT INTO meta (version, project, run_id, last_synced_operation)
            VALUES (?, ?, ?, ?);""",
            ("1", project, run_id, 0),
        )


def _safe_filename(project: str, run_id: str) -> str:
    # URLEncode the project name and run ID to avoid issues with special characters
    filename = quote(f"{project}-{run_id}.db", safe="")
    return filename


def _data_dir(base_dir: Optional[str] = None) -> Path:
    if base_dir is None:
        base_dir = ""

    return Path(base_dir) / DATA_DIR
