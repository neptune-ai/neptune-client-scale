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
