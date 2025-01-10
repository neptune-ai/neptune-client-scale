import os
from typing import Optional

PROJECT_ENV_NAME = "NEPTUNE_PROJECT"

API_TOKEN_ENV_NAME = "NEPTUNE_API_TOKEN"

DISABLE_COLORS = "NEPTUNE_DISABLE_COLORS"

DEBUG_MODE = "NEPTUNE_DEBUG_MODE"

SUBPROCESS_KILL_TIMEOUT = "NEPTUNE_SUBPROCESS_KILL_TIMEOUT"

ALLOW_SELF_SIGNED_CERTIFICATE = "NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE"
SKIP_NON_FINITE_METRICS = "NEPTUNE_SKIP_NON_FINITE_METRICS"

BASE_STORAGE_DIR = "NEPTUNE_BASE_STORAGE_DIR"


def get_bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).lower() in ("true", "1")


def get_str(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(name, default)
