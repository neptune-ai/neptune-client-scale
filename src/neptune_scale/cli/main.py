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
from typing import Optional

import click

from neptune_scale.cli import (
    misc,
    sync,
)
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
main.add_command(misc.status)

if __name__ == "__main__":
    main()
