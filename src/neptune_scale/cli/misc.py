import click

from neptune_scale.cli.util import is_neptune_dir
from neptune_scale.storage.operations import list_runs


@click.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    if not is_neptune_dir(ctx.obj["neptune_dir"]):
        return

    for run in list_runs(ctx.obj["neptune_dir"]):
        print(f"{run.path}: Project: {run.project}, Run ID: {run.run_id}, {run.operation_count} operations")
