import os
from typing import Optional

PROJECT_ENV_NAME = "NEPTUNE_PROJECT"

API_TOKEN_ENV_NAME = "NEPTUNE_API_TOKEN"

DISABLE_COLORS = "NEPTUNE_DISABLE_COLORS"

DEBUG_MODE = "NEPTUNE_DEBUG_MODE"

SUBPROCESS_KILL_TIMEOUT = "NEPTUNE_SUBPROCESS_KILL_TIMEOUT"

ALLOW_SELF_SIGNED_CERTIFICATE = "NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE"
SKIP_NON_FINITE_METRICS = "NEPTUNE_SKIP_NON_FINITE_METRICS"

FREE_QUEUE_SLOT_TIMEOUT_SECS = "NEPTUNE_FREE_QUEUE_SLOT_TIMEOUT_SECS"
LOG_FAILURE_ACTION = "NEPTUNE_LOG_FAILURE_ACTION"


def get_bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).lower() in ("true", "1")


def get_int(name: str, default: Optional[int], positive: bool = False) -> Optional[int]:
    """Get int value from env, returning the default if not found, or if the value is not an int. If positive is
    True and the value is not positive, the default is returned."""
    try:
        value = int(os.getenv(name, str(default)))
        if positive and value <= 0:
            return default
        return value
    except ValueError:
        return default
