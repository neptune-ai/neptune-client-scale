#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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
#

__all__ = ["sync"]

import os
from pathlib import Path
from typing import (
    Optional,
)

import click

from neptune_scale.cli.sync import sync_all
from neptune_scale.util.envs import API_TOKEN_ENV_NAME


@click.group()
def main() -> None:
    pass


@main.command()
@click.argument(
    "run_log_file",
    type=click.Path(exists=True, dir_okay=False, resolve_path=True),
    metavar="<run-log-file>",
)
@click.option(
    "--api-token",
    "api_token",
    multiple=False,
    default=os.environ.get(API_TOKEN_ENV_NAME),
    metavar="<api-token>",
    help="API token for authentication. Overrides NEPTUNE_API_TOKEN environment variable",
)
@click.option(
    "--sync-no-parent",
    "sync_no_parent",
    is_flag=True,
    default=False,
    help="Override error if the run is being created as a fork and the parent doesn't exist",
)
def sync(
    run_log_file: Path,
    api_token: Optional[str],
    sync_no_parent: bool,
) -> None:
    sync_all(run_log_file, api_token, sync_no_parent)