import os
import sqlite3
import threading
from pathlib import Path
from typing import Optional
from urllib.parse import quote

DATA_DIR = ".neptune"


class OperationStorageBase:
    def __init__(self, project: str, run_id: str, db_path: Optional[Path] = None) -> None:
        self._project = project
        self._run_id = run_id
        self._db_path = db_path
        self._db: Optional[sqlite3.Connection] = None
        self._lock = threading.RLock()

    def init_db(self) -> None:
        with self._lock:
            if self._db_path is None:
                self._db_path = init_storage(self._project, self._run_id)

            self._db = sqlite3.connect(self._db_path, autocommit=False)


class OperationWriter(OperationStorageBase):
    def write(self, serialized_op: bytes) -> None:
        assert self._db  # mypy
        with self._lock, self._db:
            self._db.execute(
                """
                INSERT INTO operations (run_id, operation)
                VALUES (?, ?);""",
                (self._run_id, serialized_op),
            )

    def close(self) -> None:
        with self._lock:
            if self._db:
                self._db.close()
                self._db = None


class OperationReader(OperationStorageBase): ...


def _data_dir(base_dir: Optional[str] = None) -> Path:
    if base_dir is None:
        base_dir = "."

    return Path(base_dir) / DATA_DIR


def _safe_filename(project: str, run_id: str) -> str:
    # URLEncode the project name and run ID to avoid issues with special characters
    filename = quote(f"{project}-{run_id}.db", safe="")
    return filename


def database_path_for_run(project: str, run_id: str, base_dir: Optional[str] = None) -> Path:
    return _data_dir(base_dir) / _safe_filename(project, run_id)


def init_storage(project: str, run_id: str, base_dir: Optional[str] = None) -> Path:
    """Initializes the local SQLite storage for writing operations. This function is called by Run in the
    main process, to give user a chance to react early to any storage-related errors.

    Returns:
        path to the SQLite store
    """

    data_dir = _data_dir(base_dir)
    os.makedirs(data_dir, exist_ok=True)

    path = database_path_for_run(project, run_id, base_dir)
    if path.exists():
        # TODO: validate tables if they exist / add a migration hook
        return path

    with sqlite3.connect(path) as conn:
        init_db(conn)

    return path


def init_db(db: sqlite3.Connection) -> None:
    with db:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS operations (
                run_id BLOB NOT NULL,
                seq INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                operation BLOB NOT NULL,
                UNIQUE(run_id, seq)
            );"""
        )

        db.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
                version TEXT NOT NULL,
                run_id BLOB NOT NULL,
                last_synced_operation INTEGER,
                PRIMARY KEY (run_id),
                FOREIGN KEY (run_id, last_synced_operation) REFERENCES operations(run_id, seq)
            );"""
        )
