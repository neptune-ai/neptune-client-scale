from pathlib import Path

from neptune_scale.util import get_logger

logger = get_logger()


def is_neptune_dir(path: Path) -> bool:
    if not path.exists():
        return False

    if not path.is_dir():
        logger.warning(f"Expected {path} to be a readable directory")
        return False

    return True


def format_duration(seconds: int) -> str:
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)

    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or not parts:
        parts.append(f"{seconds}s")

    return " ".join(parts)
