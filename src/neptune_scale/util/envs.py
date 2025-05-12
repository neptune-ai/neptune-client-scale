import os
from typing import Union

PROJECT_ENV_NAME = "NEPTUNE_PROJECT"

API_TOKEN_ENV_NAME = "NEPTUNE_API_TOKEN"

DISABLE_COLORS = "NEPTUNE_DISABLE_COLORS"

# Deprecated in favour of NEPTUNE_LOGGER_LEVEL
DEBUG_MODE = "NEPTUNE_DEBUG_MODE"

# Logging level for the neptune logger. Valid values: "debug", "info", "warning", "error", "critical", "none".
LOGGER_LEVEL = "NEPTUNE_LOGGER_LEVEL"

SUBPROCESS_KILL_TIMEOUT = "NEPTUNE_SUBPROCESS_KILL_TIMEOUT"

ALLOW_SELF_SIGNED_CERTIFICATE = "NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE"
SKIP_NON_FINITE_METRICS = "NEPTUNE_SKIP_NON_FINITE_METRICS"

LOG_MAX_BLOCKING_TIME_SECONDS = "NEPTUNE_LOG_MAX_BLOCKING_TIME_SECONDS"
LOG_FAILURE_ACTION = "NEPTUNE_LOG_FAILURE_ACTION"

LOG_DIRECTORY = "NEPTUNE_LOG_DIRECTORY"
MAX_CONCURRENT_FILE_UPLOADS = "NEPTUNE_MAX_CONCURRENT_FILE_UPLOADS"

MODE_ENV_NAME = "NEPTUNE_MODE"


def get_bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).lower() in ("true", "1")


def get_positive_int(name: str, default: int) -> int:
    """Get int value from env, returning the default if not found. If the value is not an int, raise ValueError."""

    value = os.getenv(name)
    try:
        int_value = default if value is None else int(value)
        if int_value <= 0:
            raise ValueError()
        return int_value
    except ValueError:
        raise ValueError(f"{name} must be a positive integer, got '{value}'")


def get_option(name: str, choices: Union[list[str], tuple[str, ...]], default: str) -> str:
    """Get a string from env, returning the default if not found.
    If the value is not in `choices`, raise ValueError.
    The value is returned lowercase. The `choices` iterable should hold lowercase strings."""

    assert default in choices

    value = os.getenv(name)
    if value is None:
        return default

    value_lower = value.lower()
    if value_lower not in choices:
        raise ValueError(f"{name} must be one of: {', '.join(choices)}, got '{value}'")

    return value_lower
