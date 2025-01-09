import os
import sqlite3
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import (
    datetime,
    timezone,
)
from pathlib import Path
from typing import (
    Optional,
    Union,
)
from urllib.parse import quote

from neptune_scale.util import get_logger

DATA_DIR = ".neptune"
BATCH_SIZE = 10000

logger = get_logger()


@dataclass
class LocalRun:
    project: str
    run_id: str
    operation_count: int
    creation_time: datetime
    last_synced_operation: int
    path: Path

    experiment_name: Optional[str]
    fork_run_id: Optional[str]
    fork_step: Optional[Union[int, float]]

    @classmethod
    def from_db(cls, conn: sqlite3.Connection, path: Path) -> "LocalRun":
        row = conn.execute(
            """
            SELECT
                project, run_id, creation_time, last_synced_operation, experiment_name, fork_run_id, fork_step
            FROM meta"""
        ).fetchone()

        project, run_id, creation_time, last_synced_op, experiment_name, fork_run_id, fork_step = row

        # Basic sanity check
        if project is None or run_id is None:
            raise ValueError(f"Invalid Neptune database at {path}")

        creation_time = datetime.fromtimestamp(creation_time, tz=timezone.utc)

        count = conn.execute("SELECT count(*) FROM operations WHERE run_id = ?", (run_id,)).fetchone()[0]

        return cls(
            project,
            run_id,
            count,
            creation_time,
            last_synced_op,
            path,
            experiment_name,
            fork_run_id,
            fork_step,
        )


@dataclass
class Operation:
    run_id: str
    seq: int
    data: bytes


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

    def init_db(self) -> None:
        with self._lock:
            if self._resume and not os.path.isfile(self._db_path):
                raise FileNotFoundError(f"Neptune Database not found at {self._db_path}")

            # We use `check_same_thread=False` because closing the DB could be called from different
            # threads. It's fine, as long as we do our own synchronization.
            self._db = sqlite3.connect(self._db_path, check_same_thread=False)
            if self._resume:
                self._last_synced_op = self._db.execute(
                    "SELECT last_synced_operation FROM meta WHERE run_id = ?", (self._run_id,)
                ).fetchone()[0]

    def write(self, serialized_op: bytes) -> None:
        """Store the given operation. It's assumed that `serialized_op` is a valid serialized RunOperation.

        The operation is NOT committed to the database immediately, to allow for batching. Use `commit()` to
        do this explicitly. Calling close() commits all pending operations as well.
        """

        assert self._db  # mypy
        with self._lock:
            self._db.execute(
                """
                INSERT INTO operations (run_id, operation)
                VALUES (?, ?);""",
                (self._run_id, serialized_op),
            )

    def commit(self) -> None:
        assert self._db  # mypy
        with self._lock:
            self._db.commit()

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
                self._db.commit()
                self._db.close()
                self._db = None


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

        self._db = sqlite3.connect(db_path, check_same_thread=False)
        self.run = LocalRun.from_db(self._db, Path(db_path))

    @property
    def pending_operations_count(self) -> int:
        return self.run.operation_count - self.run.last_synced_operation

    @property
    def completed_operations_count(self) -> int:
        return self.run.last_synced_operation

    @property
    def all_operations(self) -> Iterator[Operation]:
        yield from self._operations(0)

    @property
    def pending_operations(self) -> Iterator[Operation]:
        yield from self._operations(self.run.last_synced_operation + 1)

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
                    (self.run.run_id, start_seq, BATCH_SIZE, offset),
                ).fetchall()

            for run_id, seq, op in batch:
                yield Operation(run_id, seq, op)

            if len(batch) < BATCH_SIZE:
                break

            offset += len(batch)


def database_path_for_run(project: str, run_id: str, base_dir: Optional[str] = None) -> Path:
    return _data_dir(base_dir) / _safe_filename(project, run_id)


def init_write_storage(
    project: str,
    run_id: str,
    base_dir: Optional[str] = None,
    *,
    creation_time: Optional[datetime] = None,
    experiment_name: Optional[str] = None,
    fork_run_id: Optional[str] = None,
    fork_step: Optional[Union[int, float]] = None,
) -> Path:
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
        _init_run(
            conn,
            project,
            run_id,
            resume=False,
            creation_time=creation_time,
            experiment_name=experiment_name,
            fork_run_id=fork_run_id,
            fork_step=fork_step,
        )

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
                creation_time INTEGER NOT NULL,
                experiment_name TEXT,
                fork_run_id TEXT,
                fork_step REAL,
                last_synced_operation INTEGER,
                PRIMARY KEY (run_id),
                FOREIGN KEY (run_id, last_synced_operation) REFERENCES operations(run_id, seq)
            )"""
        )


def _init_run(
    db: sqlite3.Connection,
    project: str,
    run_id: str,
    *,
    resume: bool,
    creation_time: Optional[datetime] = None,
    experiment_name: Optional[str] = None,
    fork_run_id: Optional[str] = None,
    fork_step: Optional[Union[int, float]] = None,
) -> None:
    with db:
        row = db.execute("SELECT 1 FROM meta WHERE run_id = ?;", (run_id,)).fetchone()
        if row is None:
            if resume:
                raise ValueError(f"Run {run_id} does not exist in local storage")
        else:
            if not resume:
                raise ValueError(f"Run {run_id} already exists in local storage")
            return

        # Note that storing `fork_step` as REAL is safe in terms of precision errors, so we don't need to
        # do anything like converting to TEXT back and forth.
        db.execute(
            """
            INSERT INTO meta (
                version, project, run_id, creation_time, experiment_name,
                fork_run_id, fork_step, last_synced_operation
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);""",
            ("1", project, run_id, creation_time or int(time.time()), experiment_name, fork_run_id, fork_step, 0),
        )


def _safe_filename(project: str, run_id: str) -> str:
    # URLEncode the project name and run ID to avoid issues with special characters
    filename = quote(f"{project}-{run_id}.db", safe="")
    return filename


def _data_dir(base_dir: Optional[str] = None) -> Path:
    if base_dir is None:
        base_dir = ""

    return Path(base_dir) / DATA_DIR


def list_runs(path: Path) -> Iterator[LocalRun]:
    if not path.is_dir():
        raise ValueError(f"{path} is not a readable directory")

    for filename in path.glob("*.db"):
        try:
            # Need to quote the filename, as we're passing it as URI. This is the only way
            # to open the DB in read-only mode.
            uri = f"file:{quote(str(filename))}?mode=ro"
            with sqlite3.connect(uri, uri=True) as db:
                yield LocalRun.from_db(db, filename)

        except Exception as e:
            logger.warning(f"Skipping {filename}: {e}")
