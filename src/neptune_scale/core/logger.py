__all__ = ("logger",)

import logging
import os

from neptune_scale.core.styles import (
    STYLES,
    ensure_style_detected,
)
from neptune_scale.envs import DEBUG_MODE

NEPTUNE_LOGGER_NAME = "neptune"
NEPTUNE_DEBUG_FILE_NAME = "neptune.log"
LOG_FORMAT = "{blue}%(name)s{end} :: {bold}%(levelname)s{end} :: %(message)s"
DEBUG_FORMAT = "%(asctime)s :: %(name)s :: %(levelname)s :: %(processName)s(%(process)d):%(filename)s:%(funcName)s():%(lineno)d %(message)s"


def get_logger() -> logging.Logger:
    ensure_style_detected()

    neptune_logger = logging.getLogger(NEPTUNE_LOGGER_NAME)
    neptune_logger.setLevel(logging.INFO)

    if os.environ.get(DEBUG_MODE, "False").lower() in ("true", "1"):
        neptune_logger.setLevel(logging.DEBUG)

        file_handler = logging.FileHandler(NEPTUNE_DEBUG_FILE_NAME)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(DEBUG_FORMAT))
        neptune_logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter(LOG_FORMAT.format(**STYLES)))
    neptune_logger.addHandler(stream_handler)

    return neptune_logger


logger = get_logger()
