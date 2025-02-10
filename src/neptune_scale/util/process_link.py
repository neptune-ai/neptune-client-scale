import multiprocessing
import os
import queue
import threading
from collections.abc import Callable
from functools import partial
from multiprocessing.connection import Connection
from typing import (
    Any,
    Optional,
)

from neptune_scale.util import (
    Daemon,
    get_logger,
)

POLL_TIMEOUT = 0.1

logger = get_logger()


class ProcessLink:
    """Used for bidirectional monitoring and control channel between two processes.

    When a linked process terminates, the `on_link_closed` callback provided in `start()` will be called.
    This way both the parent and child process know when any of them terminated.

    Note that:
     * you MUST call `start()` on the link in both parent and child processes
     * the callback will be called from a thread

    Typically, you would use it like this:
        import time

        # Child process worker
        def worker(link):
            # Required for the link to work
            link.start()
            # Do some work
            time.sleep(1)

        # Parent process
        link = ProcessLink()
        child = multiprocessing.Process(target=worker, args=(link,))
        child.start()

        link.start(on_link_closed=lambda l: print('child closed'))
        link.join() # Wait until the other side is terminated

    See tests/unit/test_process_link.py for more examples.

    The main idea behind this is to create pipes shared between child and parent processes.
    When any process terminates, the other one will receive errors when reading the pipe.

    We could use the `signal` module (registering for SIGCHLD), but this approach has several problems:
     - registering for SIGCHLD is not available on Windows
     - registering for SIGCHLD is brittle, because users might register their own signal handlers, which would either
       break our library, or user code
     - the child process has no way to detect if the parent process dies
    """

    def __init__(self) -> None:
        self._pipes = multiprocessing.Pipe()

        self._on_link_closed: Optional[Callable[[ProcessLink], Any]] = None
        self._on_message_received: Optional[Callable[[ProcessLink, Any], Any]] = None

        # PID of the process that created this object, which should be the parent process.
        # Used to detect if we're calling start() from child or parent.
        self._parent_pid = os.getpid()

        # These fields below are initialized in the start() method, as they cannot be pickled
        # and thus passed to a multiprocessing.Process()

        self._worker: Optional[ProcessLinkWorker] = None
        # Signal end of the startup procedure
        self._start_done: Optional[threading.Event] = None
        self._msg_queue: Optional[queue.SimpleQueue] = None

    def start(
        self,
        *,
        timeout: float = 5.0,
        on_link_closed: Optional[Callable[["ProcessLink"], Any]] = None,
        on_message_received: Optional[Callable[["ProcessLink", Any], Any]] = None,
    ) -> bool:
        """Start the monitoring thread and wait for `timeout` seconds for the other side to start.

        Returns True if the other side is started successfully within the given timeout, False otherwise.

        You can provide callbacks for when the link is closed or when a message is received. The callbacks should
        accept the `ProcessLink` instance as the first argument.

        Note that during this call the `on_terminate` callback can already be called, for example
        if the process fails to start properly.

        This method is NOT thread-safe. It should be called only once, preferably from the main thread.
        """

        if self._worker:
            raise RuntimeError("start() can only be called once")

        self._on_link_closed = on_link_closed
        self._on_message_received = on_message_received
        self._msg_queue = queue.SimpleQueue()

        # Close the "other" end of the pipe depending on which process we're in
        if os.getpid() == self._parent_pid:
            self._pipes[1].close()
            conn = self._pipes[0]
        else:
            self._pipes[0].close()
            conn = self._pipes[1]

        del self._pipes
        self._start_done = threading.Event()

        self._worker = ProcessLinkWorker(
            conn,
            self._start_done,
            start_timeout=timeout,
            msg_queue=self._msg_queue,
            on_link_closed=partial(self._on_link_closed, self) if self._on_link_closed else None,
            on_message_received=partial(self._on_message_received, self) if self._on_message_received else None,
        )
        self._worker.start()

        return self._start_done.wait(timeout=timeout) and not self._worker.is_link_closed

    def stop(self, wait: bool = True, timeout: Optional[float] = None) -> None:
        """Stop monitoring the link. The other side will be notified. The `on_link_closed` callback will NOT be called
        in the calling process. You can optionally wait for the worker to finish.
        """

        if not self._worker:
            raise RuntimeError("You must call start() before calling stop()")

        self._worker.stop_link()
        if wait:
            self.join(timeout)

    def join(self, timeout: Optional[float] = None) -> None:
        """
        Wait for the link monitor to finish. This happens only after the other side is terminated or stop() is called.
        """

        if not self._worker:
            raise RuntimeError("You must call start() before calling join()")

        self._worker.join(timeout=timeout)

    def send(self, message: Any) -> None:
        if not self._msg_queue:
            raise RuntimeError("You must call start() before sending messages")

        self._msg_queue.put(message)


class ProcessLinkWorker(Daemon):
    def __init__(
        self,
        conn: Connection,
        start_done_event: threading.Event,
        start_timeout: float,
        msg_queue: queue.SimpleQueue,
        *,
        on_link_closed: Optional[Callable[[], Any]] = None,
        on_message_received: Optional[Callable[[Any], Any]] = None,
    ):
        super().__init__(sleep_time=0, name="ProcessLink")

        # Indicates if the other side is alive
        self._closed = False

        self._conn = conn
        self._start_timeout = start_timeout
        self._msg_queue = msg_queue
        self._on_link_closed = on_link_closed
        self._on_message_received = on_message_received

        # Signal end of the startup procedure
        self._start_done = start_done_event
        # Set if we've started stopping the link
        self._stop_event = threading.Event()
        self._lock = threading.RLock()

    @property
    def is_link_closed(self) -> bool:
        with self._lock:
            return self._closed

    def run(self) -> None:
        # Before entering the main loop we send and receive a single message.
        # This allows us to make sure that the process on the other end of the pipe is started successfully.
        success = False
        try:
            self._conn.send("started")

            # Give the child process some time for startup
            if not self._conn.poll(timeout=self._start_timeout):
                raise RuntimeError(f"Other side failed to start within {self._start_timeout} seconds")

            if (msg := self._conn.recv()) != "started":
                raise RuntimeError(f"Invalid challenge response: {msg}")
            success = True
        except Exception as e:
            logger.error(f"{self}: unexpected error while sending data: {e}")
            with self._lock:
                self._closed = True
            self.interrupt()

        self._start_done.set()

        # ProcessLink is guaranteed not to call on_link_close after stop() has been called so make sure it is so
        if not success and not self._stop_event.is_set():
            self._safe_callback(self._on_link_closed)
        else:
            super().run()

    def work(self) -> None:
        if self._stop_event.is_set():
            # This will notify the other side about us closing the link
            if not self._conn.closed:
                self._conn.close()

            self.interrupt()
            return

        try:
            self._send_queued_messages()

            # Don't block on self._conn.recv() indefinitely, so we can react to stopping the link
            if not self._conn.poll(timeout=POLL_TIMEOUT) or not self.is_running():
                return

            msg = self._conn.recv()
            if not self._stop_event.is_set():
                self._safe_callback(self._on_message_received, msg)
        except Exception as e:
            # EOFError is expected when the other side closes the connection.
            if not isinstance(e, EOFError):
                logger.error(f"{self}: unexpected error while receiving data: {e}")

            with self._lock:
                self._closed = True

            if not self._stop_event.is_set():
                self._safe_callback(self._on_link_closed)
                self._conn.close()

            self.interrupt()

    def stop_link(self) -> None:
        self._stop_event.set()

    def _safe_callback(self, func: Optional[Callable[..., Any]], *args: Any, **kwargs: Any) -> None:
        if func is None:
            return

        try:
            func(*args, **kwargs)
        except Exception as e:
            logger.error(f"{self}: exception while calling user callback {func}: {e}")

    def _send_queued_messages(self) -> None:
        while True:
            try:
                out = self._msg_queue.get_nowait()
                self._conn.send(out)
            except queue.Empty:
                return
