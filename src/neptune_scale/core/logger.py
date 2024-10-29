__all__ = (
    "init_main_process_logger",
    "init_child_process_logger",
    "get_logger",
)

import atexit
import logging
import multiprocessing
import os
from logging.handlers import (
    QueueHandler,
    QueueListener,
)
from typing import (
    List,
    Tuple,
)

from neptune_scale.core.styles import (
    STYLES,
    ensure_style_detected,
)
from neptune_scale.envs import DEBUG_MODE

NEPTUNE_LOGGER_NAME = "neptune"
NEPTUNE_DEBUG_FILE_NAME = "neptune.log"
LOG_FORMAT = "{blue}%(name)s{end} :: {bold}%(levelname)s{end} :: %(message)s"
DEBUG_FORMAT = "%(asctime)s :: %(name)s :: %(levelname)s :: %(processName)s(%(process)d):%(threadName)s:%(filename)s:%(funcName)s():%(lineno)d %(message)s"


def get_logger() -> logging.Logger:
    """Use in modules to get the root Neptune logger"""
    return logging.getLogger(NEPTUNE_LOGGER_NAME)


def init_main_process_logger() -> Tuple[logging.Logger, multiprocessing.Queue]:
    """
    Initialize the root 'neptune' logger to use a mp.Queue. This should be called only once in the main process.

    Returns:
        A 2-tuple of (logger, queue). Queue should be passed to `init_child_logger()` in child processes.
    """

    if multiprocessing.parent_process() is not None:
        raise RuntimeError("This function should be called only in the main process.")

    logger = logging.getLogger(NEPTUNE_LOGGER_NAME)

    # If the user has also imported `neptune-fetcher` the root logger will already be initialized.
    # We want our handlers to take precedence. We will remove all handlers and add our own.
    if logger.hasHandlers():
        # Not initialized by us, clear handlers and proceed.
        if not hasattr(logger, "__neptune_scale_listener"):
            logger.handlers.clear()
        else:
            return logger, _get_queue_from_handlers(logger.handlers)

    logger.setLevel(logging.INFO)
    ensure_style_detected()

    handlers: List[logging.Handler] = []
    if os.environ.get(DEBUG_MODE, "False").lower() in ("true", "1"):
        logger.setLevel(logging.DEBUG)

        file_handler = logging.FileHandler(NEPTUNE_DEBUG_FILE_NAME)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(DEBUG_FORMAT))
        handlers.append(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter(LOG_FORMAT.format(**STYLES)))
    handlers.append(stream_handler)

    queue: multiprocessing.Queue = multiprocessing.Queue()
    logger.addHandler(QueueHandler(queue))

    listener = QueueListener(queue, *handlers, respect_handler_level=True)
    listener.start()

    logger.__neptune_scale_listener = listener  # type: ignore
    atexit.register(listener.stop)

    return logger, queue


def init_child_process_logger(queue: multiprocessing.Queue) -> logging.Logger:
    """
    Initialize a child logger to use the given queue. This should be called in child processes only once.
    After it's called, the logger can be retrieved using `get_logger()`.

    Args:
        queue: A multiprocessing.Queue object returned by `init_root_logger()`.

    Returns:
        A logger instance.
    """

    if multiprocessing.parent_process() is None:
        raise RuntimeError("This function should be called only in child processes.")

    logger = logging.getLogger(NEPTUNE_LOGGER_NAME)
    if logger.hasHandlers():
        # Make sure the QueueHandler is already registered
        _ = _get_queue_from_handlers(logger.handlers)
        return logger

    ensure_style_detected()

    logger.addHandler(QueueHandler(queue))
    logger.setLevel(logging.INFO)

    return logger


def _get_queue_from_handlers(handlers: List[logging.Handler]) -> multiprocessing.Queue:
    for h in handlers:
        if isinstance(h, QueueHandler):
            return h.queue  # type: ignore # mypy doesn't know it's always Queue

    raise RuntimeError("Expected to find a QueueHandler in the logger handlers.")
