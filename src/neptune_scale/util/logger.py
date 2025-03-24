__all__ = ("get_logger",)

import logging

from neptune_scale.util import envs
from neptune_scale.util.envs import DEBUG_MODE
from neptune_scale.util.styles import (
    STYLES,
    ensure_style_detected,
)


class NeptuneWarning(Warning): ...


LOG_FORMAT = "%(asctime)s {blue}%(name)s{end}:{bold}%(levelname)s{end}: %(message)s"
DEBUG_FORMAT = "%(asctime)s:%(name)s:%(levelname)s:%(processName)s/%(threadName)s/%(funcName)s: %(message)s"


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

    ensure_style_detected()

    debug = envs.get_bool(DEBUG_MODE, False)
    logger.setLevel(logging.DEBUG if debug else logging.INFO)

    stream_handler = logging.StreamHandler()
    log_format = DEBUG_FORMAT if debug else LOG_FORMAT.format(**STYLES)
    stream_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(stream_handler)

    logger.__neptune_scale = True  # type: ignore

    return logger
