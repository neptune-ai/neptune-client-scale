from __future__ import annotations

__all__ = ("SQLiteDiskQueue",)

import os
import sqlite3
import threading
from typing import Optional

from neptune_scale.util import get_logger

logger = get_logger()


class SQLiteDiskQueue:
    """A disk-based queue implementation using SQLite.

    This queue stores RunOperation objects in a SQLite database with the following schema:
    - Table `run_operations`: Contains serialized RunOperation objects
      - operation_id INTEGER: Auto-incremented operation ID (Primary Key)
      - operation BLOB: Serialized RunOperation object
    """

    def __init__(self, db_path: str) -> None:
        """Initialize the SQLite disk queue.

        Args:
            db_path: Path to the SQLite database file
        """
        self._db_path = db_path
        self._lock = threading.RLock()
        self._connection: Optional[sqlite3.Connection] = None

        # Ensure directory exists
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)

        # Initialize database
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the database schema if it doesn't exist."""
        conn = self._get_connection()
        cursor = conn.cursor()
        # Create run_operations table if it doesn't exist
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS run_operations (
                operation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation BLOB NOT NULL
            )
        """
        )

        conn.commit()

    def _get_connection(self) -> sqlite3.Connection:
        with self._lock:
            if self._connection is None:
                # Create a new connection
                self._connection = sqlite3.connect(
                    self._db_path,
                    check_same_thread=False,  # we use RLock to synchronize access
                )
                # Use WAL mode for better concurrency
                self._connection.execute("PRAGMA journal_mode = WAL")
                logger.debug(f"Created new SQLite connection for {self._db_path}")

            return self._connection

    def enqueue(self, element: bytes) -> None:
        """Enqueue a single operation.

        Args:
            element: Serialized operation to enqueue

        Returns:
            The operation_id of the enqueued operation
        """
        return self.enqueue_many([element])

    def enqueue_many(self, elements: list[bytes]) -> None:
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            # Convert each element to a tuple for executemany
            params = [(element,) for element in elements]
            cursor.executemany("INSERT INTO run_operations (operation) VALUES (?)", params)
            conn.commit()

    def peek(self, limit: int = 1) -> list[tuple[int, bytes]]:
        """Peek at operations without removing them.

        Args:
            limit: Maximum number of operations to peek at

        Returns:
            A list of tuples containing (operation_id, serialized_operation)
        """
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT operation_id, operation FROM run_operations ORDER BY operation_id ASC LIMIT ?",
                (limit,),
            )
            return cursor.fetchall()

    def delete_operations(self, operation_ids: list[int]) -> int:
        """Delete multiple operations by their IDs.

        Args:
            operation_ids: A list of operation IDs to delete

        Returns:
            The number of operations that were deleted
        """
        if not operation_ids:
            return 0

        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Create placeholders for the SQL query
            placeholders = ", ".join("?" for _ in operation_ids)

            # Execute the delete query with the list of operation IDs
            cursor.execute(
                f"DELETE FROM run_operations WHERE operation_id IN ({placeholders})",
                operation_ids,
            )
            conn.commit()
            # Return the number of rows affected
            return cursor.rowcount

    def get_operation_count(self) -> int:
        """Get the number of operations in the queue.

        Returns:
            The number of operations
        """
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM run_operations")
            count: int = cursor.fetchone()[0]
            return count

    def close(self) -> None:
        """Close the database connection."""
        with self._lock:
            if self._connection is not None:
                self._connection.close()
                self._connection = None
                logger.debug(f"Closed SQLite connection for {self._db_path}")
