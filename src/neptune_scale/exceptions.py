from __future__ import annotations

__all__ = (
    "NeptuneScaleError",
    "NeptuneOperationsQueueMaxSizeExceeded",
    "NeptuneUnauthorizedError",
    "NeptuneInvalidCredentialsError",
    "NeptuneUnexpectedError",
    "NeptuneConnectionLostError",
    "NeptuneUnableToAuthenticateError",
    "NeptuneRetryableError",
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
The queue size for internal operations was exceeded (max allowed: {max_size}) because too much data was queued in a short time.

The synchronization is paused until the queue size drops below the maximum.

To resolve this issue, consider the following:
    - Reduce the frequency of data being sent to the queue, or throttle the rate of operations.
    - Cautiously increase the queue size through the `max_queue_size` argument.
        Note: To ensure that memory usage remains within acceptable limits,
        closely monitor your system's memory consumption.

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneUnauthorizedError(NeptuneScaleError):
    message = """
{h1}
----NeptuneUnauthorizedError---------------------------------------------------
{end}
You don't have permission to access the given resource.

    - Verify that your API token is correct. To find your API token:
        - Log in to Neptune Scale and open the user menu.
        - If your workspace uses service accounts, ask the project owner to provide the token.

    - Verify that the provided project name is correct.
      The correct project name should look like this: {correct}WORKSPACE_NAME/PROJECT_NAME{end}
      It has two parts:
          - {correct}WORKSPACE_NAME{end}: can be your username or your organization name
          - {correct}PROJECT_NAME{end}: the name specified for the project

   - Ask your workspace admin to grant you the necessary privileges to the project.

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneInvalidCredentialsError(NeptuneScaleError):
    message = """
{h1}
----NeptuneInvalidCredentialsError---------------------------------------------
{end}
The provided API token is invalid.
Make sure you copied your API token while logged in to Neptune Scale.
If your workspace uses service accounts, ask the project owner for the token.

There are two options to provide the API token:
    - Set it as an environment variable in your operating system
    - Paste it into your Python code (not recommended)

{h2}Environment variable{end} {correct}(Recommended){end}
Set the NEPTUNE_API_TOKEN environment variable depending on your operating system:

    {correct}Linux/Unix{end}
    In the terminal:
        {bash}export NEPTUNE_API_TOKEN="YOUR_API_TOKEN"{end}

    {correct}Windows{end}
    In Command Prompt or similar:
        {bash}setx NEPTUNE_API_TOKEN "YOUR_API_TOKEN"{end}

and omit the {bold}api_token{end} argument from the {bold}Run{end} constructor:
    {python}neptune_scale.Run(project="WORKSPACE_NAME/PROJECT_NAME"){end}

{h2}Option 2: Run argument{end}
Pass the token to the {bold}Run{end} constructor via the {bold}api_token{end} argument:
    {python}neptune_scale.Run(project="WORKSPACE_NAME/PROJECT_NAME", api_token="YOUR_API_TOKEN"){end}

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneUnexpectedError(NeptuneScaleError):
    message = """
{h1}
----NeptuneUnexpectedError-----------------------------------------------------
{end}
An unexpected error occurred in Neptune Scale Client. Please contact Neptune.ai support. Raw exception name: "{reason}"

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""

    def __init__(self, reason: str) -> None:
        super().__init__(reason=reason)


class NeptuneRetryableError(NeptuneScaleError):
    pass


class NeptuneConnectionLostError(NeptuneRetryableError):
    message = """
{h1}
----NeptuneConnectionLostError-------------------------------------------------
{end}
The connection to the Neptune server was lost. Check if your computer is connected to the internet or whether
    firewall settings are blocking the connection.

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneUnableToAuthenticateError(NeptuneScaleError):
    message = """
{h1}
----NeptuneUnableToAuthenticateError-------------------------------------------
{end}
The client was unable to authenticate with the Neptune server. Check if your API token is correct.

{correct}Need help?{end}-> https://docs.neptune.ai/getting_help

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""
