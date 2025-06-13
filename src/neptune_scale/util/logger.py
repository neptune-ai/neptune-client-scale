__all__ = ("get_logger",)

import logging
import os
import time
import warnings
from typing import (
    Any,
    Optional,
)

from neptune_scale.sync.parameters import STOP_MESSAGE_FREQUENCY
from neptune_scale.util.envs import (
    DEBUG_MODE,
    LOGGER_LEVEL,
)
from neptune_scale.util.styles import (
    STYLES,
    ensure_style_detected,
)


class NeptuneWarning(Warning): ...


DEFAULT_FORMAT = "%(asctime)s {blue}%(name)s{end}:{bold}%(levelname)s{end}: %(message)s"
DEBUG_FORMAT = (
    "%(asctime)s {blue}%(name)s{end}:{bold}%(levelname)s{end}:"
    "{blue}%(processName)s/%(threadName)s/%(funcName)s{end}: %(message)s"
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

    ensure_style_detected()

    if logger_level := _logger_level_from_env():
        log_format = DEBUG_FORMAT if logger_level == "DEBUG" else DEFAULT_FORMAT

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter(log_format.format(**STYLES)))
        logger.addHandler(stream_handler)
        logger.setLevel(logger_level)
    else:
        logger.disabled = True

    logger.__neptune_scale = True  # type: ignore

    return logger


# Does not include "none", as it's a special case that is not supported by the `logging` module.
_valid_levels = ("debug", "info", "warning", "error", "critical")


def _logger_level_from_env() -> Optional[str]:
    """Return logger level as str that can be used with logging module or None if logging is disabled.
    Reads both NEPTUNE_DEBUG_MODE and NEPTUNE_LOGGER_LEVEL, with the latter taking precedence.
    """

    default_level = "info"

    debug_mode = os.environ.get(DEBUG_MODE)
    if debug_mode is not None:
        warnings.warn(
            f"{DEBUG_MODE} is deprecated and will be removed in a future release. Use {LOGGER_LEVEL} instead.",
            FutureWarning,
        )

        default_level = "debug" if debug_mode.lower() in ("true", "1") else "info"

    level = os.getenv(LOGGER_LEVEL, default_level)
    if level == "none":
        return None

    if level not in _valid_levels:
        raise ValueError(f"{LOGGER_LEVEL} must be one of: none, {', '.join(_valid_levels)}.")

    return level.upper()


class ThrottledLogger:
    def __init__(self, logger: logging.Logger, enabled: bool = True) -> None:
        self._logger = logger
        self._enabled = enabled
        self._last_print_timestamp: Optional[float] = None

    def info(self, msg: str, *args: Any) -> None:
        if not self._enabled:
            return

        current_time = time.time()

        if self._last_print_timestamp is None or current_time - self._last_print_timestamp > STOP_MESSAGE_FREQUENCY:
            self._logger.info(msg, *args)
            self._last_print_timestamp = current_time
