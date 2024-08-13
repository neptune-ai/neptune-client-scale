from __future__ import annotations

__all__ = (
    "NeptuneScaleError",
    "NeptuneOperationsQueueMaxSizeExceeded",
    "NeptuneUnauthorizedError",
    "NeptuneInvalidCredentialsError",
)

from typing import Any

from neptune_scale.core.styles import (
    STYLES,
    ensure_style_detected,
)


class NeptuneScaleError(Exception):
    message = "An error occurred in Neptune Scale Client."

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ensure_style_detected()
        super().__init__(self.message.format(*args, **STYLES, **kwargs))


class NeptuneOperationsQueueMaxSizeExceeded(NeptuneScaleError):
    message = """
{h1}
----NeptuneOperationsQueueMaxSizeExceeded--------------------------------------
{end}
The internal operations queue size has been exceeded. The current operations queue has reached the maximum allowed
    size of {max_size} operations. The synchronization will block until the queue size drops below the maximum.

Too much data was sent to the queue in a short time, causing the system to hit its operational limit.

To resolve this issue, please consider the following:
    - Reduce the frequency of data being sent to the queue, or throttle the rate of operations.
    - If appropriate, increase the queue size with `max_queue_size` parameter cautiously,
        ensuring that memory usage remains within acceptable limits.
    - Monitor your system’s memory consumption closely when adjusting the queue size.

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help

Struggling with the formatting? Disable it by setting the environment variable `NEPTUNE_DISABLE_COLORS` to `True`.
"""


class NeptuneUnauthorizedError(NeptuneScaleError):
    message = """
{h1}
----NeptuneUnauthorizedError---------------------------------------------------
{end}
You don't have permission to access the given resource.

    - Verify that your API token is correct.

    - Verify that the provided project name is correct.
      The correct project name should look like this: {correct}WORKSPACE_NAME/PROJECT_NAME{end}
      It has two parts:
          - {correct}WORKSPACE_NAME{end}: can be your username or your organization name
          - {correct}PROJECT_NAME{end}: the name specified for the project

   - Ask your organization administrator to grant you the necessary privileges to the project.

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help

Struggling with the formatting? Disable it by setting the environment variable `NEPTUNE_DISABLE_COLORS` to `True`.
"""


class NeptuneInvalidCredentialsError(NeptuneScaleError):
    message = """
{h1}
----NeptuneInvalidCredentialsError---------------------------------------------
{end}
The provided API token is invalid.
Make sure you copied and provided your API token correctly.

There are two options to add it:
    - specify it in your code
    - set it as an environment variable in your operating system.

{h2}CODE{end}
Pass the token to the {bold}Run{end} constructor via the {bold}api_token{end} argument:
    {python}neptune_scale.Run(project='WORKSPACE_NAME/PROJECT_NAME', api_token='YOUR_API_TOKEN'){end}

{h2}ENVIRONMENT VARIABLE{end} {correct}(Recommended option){end}
or export or set an environment variable depending on your operating system:

    {correct}Linux/Unix{end}
    In your terminal run:
        {bash}export NEPTUNE_API_TOKEN="YOUR_API_TOKEN"{end}

    {correct}Windows{end}
    In your CMD run:
        {bash}set NEPTUNE_API_TOKEN="YOUR_API_TOKEN"{end}

and skip the {bold}api_token{end} argument of the {bold}Run{end} constructor:
    {python}neptune_scale.Run(project='WORKSPACE_NAME/PROJECT_NAME'){end}

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help

Struggling with the formatting? Disable it by setting the environment variable `NEPTUNE_DISABLE_COLORS` to `True`.
"""
