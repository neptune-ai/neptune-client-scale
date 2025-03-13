__all__ = ("get_logger",)

import logging
import os

from neptune_scale.util.envs import DEBUG_MODE
from neptune_scale.util.styles import (
    STYLES,
    ensure_style_detected,
)


class NeptuneWarning(Warning): ...


LOG_FORMAT = "%(asctime)s {blue}%(name)s{end}:{bold}%(levelname)s{end}: %(message)s"
DEBUG_FORMAT = (
    "%(asctime)s:%(name)s:%(levelname)s:%(processName)s(%(process)d):%(threadName)s:%(filename)s:"
    "%(funcName)s():%(lineno)d: %(message)s"
)


def get_logger() -> logging.Logger:
    """Use in modules to get the root Neptune logger"""

    logger = logging.getLogger("neptune")

    # If the user has also imported `neptune-fetcher` the root logger will already be initialized.
    # We want our handlers to take precedence. We will remove all handlers and add our own.
    if logger.hasHandlers():
        # Already initialized by us
        if hasattr(logger, "__neptune_scale"):
            return logger

        # Clear handlers and proceed with initialization
        logger.handlers.clear()

    logger.setLevel(logging.INFO)
    ensure_style_detected()

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter(LOG_FORMAT.format(**STYLES)))
    logger.addHandler(stream_handler)

    if os.environ.get(DEBUG_MODE, "False").lower() in ("true", "1"):
        logger.setLevel(logging.DEBUG)

        file_handler = logging.FileHandler(f"neptune.{os.getpid()}.log")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(DEBUG_FORMAT))
        logger.addHandler(file_handler)

    logger.__neptune_scale = True  # type: ignore

    return logger
