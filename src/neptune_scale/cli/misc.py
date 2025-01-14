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

import sys

import click

from neptune_scale.cli.util import (
    format_local_run,
    is_neptune_dir,
)
from neptune_scale.storage.operations import list_runs


@click.command()
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
@click.pass_context
def status(ctx: click.Context, verbose: bool) -> None:
    if not is_neptune_dir(ctx.obj["neptune_dir"]):
        sys.exit(1)

    for run in list_runs(ctx.obj["neptune_dir"]):
        line = format_local_run(run, verbose)
        print(line)
