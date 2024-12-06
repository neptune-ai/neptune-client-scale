__all__ = ("STYLES", "ensure_style_detected")

import os
import platform

from neptune_scale.util.envs import DISABLE_COLORS

UNIX_STYLES = {
    "h1": "\033[95m",
    "h2": "\033[94m",
    "blue": "\033[94m",
    "python": "\033[96m",
    "bash": "\033[95m",
    "warning": "\033[93m",
    "correct": "\033[92m",
    "fail": "\033[91m",
    "bold": "\033[1m",
    "underline": "\033[4m",
    "end": "\033[0m",
}

WINDOWS_STYLES = {
    "h1": "",
    "h2": "",
    "blue": "",
    "python": "",
    "bash": "",
    "warning": "",
    "correct": "",
    "fail": "",
    "bold": "",
    "underline": "",
    "end": "",
}

EMPTY_STYLES = {
    "h1": "",
    "h2": "",
    "blue": "",
    "python": "",
    "bash": "",
    "warning": "",
    "correct": "",
    "fail": "",
    "bold": "",
    "underline": "",
    "end": "",
}


STYLES: dict[str, str] = {}


def ensure_style_detected() -> None:
    if not STYLES:
        if os.environ.get(DISABLE_COLORS, "False").lower() in ("true", "1"):
            STYLES.update(EMPTY_STYLES)
        else:
            if platform.system() in ["Linux", "Darwin"]:
                STYLES.update(UNIX_STYLES)
            elif platform.system() == "Windows":
                STYLES.update(WINDOWS_STYLES)
            else:
                STYLES.update(EMPTY_STYLES)
