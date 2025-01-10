import os
import signal
from typing import Optional

from neptune_scale.exceptions import NeptuneApiTokenNotProvided
from neptune_scale.util.envs import API_TOKEN_ENV_NAME


def safe_signal_name(signum: int) -> str:
    try:
        signame = signal.Signals(signum).name
    except ValueError:
        signame = str(signum)

    return signame


def ensure_api_token(api_token: Optional[str]) -> str:
    """Ensure the API token is provided via either explicit argument, or env variable."""

    api_token = api_token or os.environ.get(API_TOKEN_ENV_NAME)
    if api_token is None:
        raise NeptuneApiTokenNotProvided()

    return api_token
