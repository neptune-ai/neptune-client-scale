import signal
from datetime import (
    datetime,
    timezone,
)
from typing import (
    Optional,
    Union,
)


def safe_signal_name(signum: int) -> str:
    try:
        signame = signal.Signals(signum).name
    except ValueError:
        signame = str(signum)

    return signame


def arg_to_datetime(timestamp: Optional[Union[float, datetime]] = None) -> datetime:
    """Convert the provided float timestamp to datetime. If None, return current time in UTC."""

    if timestamp is None:
        timestamp = datetime.now()
    elif isinstance(timestamp, (float, int)):
        timestamp = datetime.fromtimestamp(timestamp, timezone.utc)
    return timestamp
