#
# Copyright (c) 2025, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
