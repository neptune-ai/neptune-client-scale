import sys

import click

from neptune_scale.cli.util import is_neptune_dir
from neptune_scale.storage.operations import list_runs


@click.command()
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
@click.pass_context
def status(ctx: click.Context, verbose: bool) -> None:
    if not is_neptune_dir(ctx.obj["neptune_dir"]):
        sys.exit(1)

    for run in list_runs(ctx.obj["neptune_dir"]):
        pct = round(run.last_synced_operation / run.operation_count * 100, 2)
        line = (
            f"Project: {run.project}, Run ID: {run.run_id}, "
            f"{run.last_synced_operation}/{run.operation_count} ({pct}%) synced"
        )
        if verbose:
            line = f"{run.path}: {line}"

        print(line)
