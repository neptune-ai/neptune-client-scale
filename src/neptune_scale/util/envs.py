import os
from typing import Optional

PROJECT_ENV_NAME = "NEPTUNE_PROJECT"

API_TOKEN_ENV_NAME = "NEPTUNE_API_TOKEN"

DISABLE_COLORS = "NEPTUNE_DISABLE_COLORS"

DEBUG_MODE = "NEPTUNE_DEBUG_MODE"

SUBPROCESS_KILL_TIMEOUT = "NEPTUNE_SUBPROCESS_KILL_TIMEOUT"

ALLOW_SELF_SIGNED_CERTIFICATE = "NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE"
SKIP_NON_FINITE_METRICS = "NEPTUNE_SKIP_NON_FINITE_METRICS"

LOG_MAX_BLOCKING_TIME_SECONDS = "NEPTUNE_LOG_MAX_BLOCKING_TIME_SECONDS"
LOG_FAILURE_ACTION = "NEPTUNE_LOG_FAILURE_ACTION"

LOG_DIRECTORY = "NEPTUNE_LOG_DIRECTORY"

MODE_ENV_NAME = "NEPTUNE_MODE"


def get_bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).lower() in ("true", "1")


def get_int(name: str, default: Optional[int] = None) -> Optional[int]:
    """Get int value from env, returning the default if not found. If the value is not an int, raise ValueError."""

    value = os.getenv(name)
    try:
        return default if value is None else int(value)
    except ValueError:
        raise ValueError(f"Environment variable {name} must be an integer, got '{value}'")
