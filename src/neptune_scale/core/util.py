import signal
from datetime import (
    datetime,
    timezone,
)


def safe_signal_name(signum: int) -> str:
    try:
        signame = signal.Signals(signum).name
    except ValueError:
        signame = str(signum)

    return signame


def ensure_utc(dt: datetime) -> datetime:
    """If `dt` has no TZ info, assume it's local time, and convert to UTC. Otherwise return as is."""

    if dt.tzinfo is None:
        return dt.astimezone(timezone.utc)

    return dt
