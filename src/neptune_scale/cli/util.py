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

from pathlib import Path

from neptune_scale.storage.operations import LocalRun
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


def format_local_run(run: LocalRun) -> str:
    if run.operation_count:
        pct = round(run.last_synced_operation / run.operation_count * 100, 2)
    else:
        pct = 100.0

    parts = [
        f"Run ID: {run.run_id}",
        f"Project: {run.project}",
        f"Synced: {run.last_synced_operation}/{run.operation_count} operations ({pct}%)",
        f"Created at: {run.creation_time}",
    ]

    if run.experiment_name:
        parts.append(f"Experiment: {run.experiment_name}")
    if run.fork_run_id:
        parts.append(f"Forked from `{run.fork_run_id}` at step {run.fork_step}")

    # parts.append(f"Path: {run.path}")

    text = "\n".join(f"  - {part}" for part in parts)
    return text
