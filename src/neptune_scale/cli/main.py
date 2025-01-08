from pathlib import Path
from typing import Optional

import click

from neptune_scale.cli import sync
from neptune_scale.storage.operations import DATA_DIR


@click.group()
@click.option(
    "--path",
    type=click.Path(exists=True, file_okay=False),
    help="Path containing Neptune data. Defaults to $CWD/.neptune",
)
@click.pass_context
def main(ctx: click.Context, path: Optional[str]) -> None:
    ctx.ensure_object(dict)
    if path is None:
        neptune_dir = Path(".") / DATA_DIR
    else:
        neptune_dir = Path(path)

    ctx.obj["neptune_dir"] = neptune_dir


main.add_command(sync.sync)
# main.add_command(misc.status)
