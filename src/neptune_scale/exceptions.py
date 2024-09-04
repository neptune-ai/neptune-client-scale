from __future__ import annotations

__all__ = (
    "NeptuneScaleError",
    "NeptuneScaleWarning",
    "NeptuneOperationsQueueMaxSizeExceeded",
    "NeptuneUnauthorizedError",
    "NeptuneInvalidCredentialsError",
    "NeptuneUnexpectedError",
    "NeptuneConnectionLostError",
    "NeptuneUnableToAuthenticateError",
    "NeptuneRetryableError",
    "NeptuneUnexpectedResponseError",
    "NeptuneInternalServerError",
    "NeptuneProjectNotFound",
    "NeptuneProjectInvalidName",
    "NeptuneRunNotFound",
    "NeptuneRunDuplicate",
    "NeptuneRunConflicting",
    "NeptuneRunForkParentNotFound",
    "NeptuneRunInvalidCreationParameters",
    "NeptuneFieldPathExceedsSizeLimit",
    "NeptuneFieldPathEmpty",
    "NeptuneFieldPathInvalid",
    "NeptuneFieldPathNonWritable",
    "NeptuneFieldTypeUnsupported",
    "NeptuneFieldTypeConflicting",
    "NeptuneSeriesPointDuplicate",
    "NeptuneSeriesStepNonIncreasing",
    "NeptuneSeriesStepNotAfterForkPoint",
    "NeptuneSeriesTimestampDecreasing",
    "NeptuneFloatValueNanInfUnsupported",
    "NeptuneStringValueExceedsSizeLimit",
    "NeptuneStringSetExceedsSizeLimit",
    "NeptuneSynchronizationStopped",
    "NeptuneAsyncLagThresholdExceeded",
)

from typing import Any

from neptune_scale.core.styles import (
    STYLES,
    ensure_style_detected,
)


class NeptuneScaleError(Exception):
    message = "An error occurred in the Neptune Scale client."

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ensure_style_detected()
        super().__init__(self.message.format(*args, **STYLES, **kwargs))


class NeptuneScaleWarning(Warning):
    message = "A warning occurred in the Neptune Scale client."

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ensure_style_detected()
        super().__init__(self.message.format(*args, **STYLES, **kwargs))


class NeptuneSynchronizationStopped(NeptuneScaleError):
    message = "Internal synchronization process was stopped."


class NeptuneOperationsQueueMaxSizeExceeded(NeptuneScaleError):
    message = """
{h1}
----NeptuneOperationsQueueMaxSizeExceeded--------------------------------------
{end}
The queue size for internal operations was exceeded because too much data was queued in a short time.

The synchronization is paused until the queue size drops below the maximum.

To resolve this issue, consider the following:
    - Reduce the frequency of data being sent to the queue, or throttle the rate of operations.
    - Cautiously increase the queue size through the `max_queue_size` argument.
        Note: To ensure that memory usage remains within acceptable limits,
        closely monitor your system's memory consumption.

{correct}Need help?{end}-> Contact support@neptune.ai

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

{correct}Need help?{end}-> Contact support@neptune.ai

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

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneUnexpectedError(NeptuneScaleError):
    message = """
{h1}
----NeptuneUnexpectedError-----------------------------------------------------
{end}
An unexpected error occurred in the Neptune Scale client. For help, contact support@neptune.ai. Raw exception name: "{reason}"

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
The connection to the Neptune server was lost. Ensure that your computer is connected to the internet and that
    firewall settings aren't blocking the connection.

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneUnexpectedResponseError(NeptuneRetryableError):
    message = """
{h1}
----NeptuneUnexpectedResponseError-------------------------------------------------
{end}
The Neptune server returned an unexpected response. This is a temporary problem.
If the problem persists, please contact us.

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneInternalServerError(NeptuneRetryableError):
    message = """
{h1}
----NeptuneInternalServerError-------------------------------------------------
{end}
We have encountered an internal server error. If the problem persists, please contact us.

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneUnableToAuthenticateError(NeptuneScaleError):
    message = """
{h1}
----NeptuneUnableToAuthenticateError-------------------------------------------
{end}
The client was unable to authenticate with the Neptune server. Ensure that your API token is correct.

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneProjectNotFound(NeptuneScaleError):
    message = """
{h1}
----NeptuneProjectNotFound-----------------------------------------------------
{end}
Project not found. Either the project hasn't been created yet or the name is incorrect.

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneProjectInvalidName(NeptuneScaleError):
    message = """
{h1}
----NeptuneProjectInvalidName--------------------------------------------------
{end}
Project name is either empty or too long.

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneRunNotFound(NeptuneScaleError):
    message = """
{h1}
----NeptuneRunNotFound---------------------------------------------------------
{end}
Run not found. May happen when the run is not yet created.

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneRunDuplicate(NeptuneScaleWarning):
    message = """
{h1}
----NeptuneRunDuplicate--------------------------------------------------------
{end}
Identical run already exists.

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneRunConflicting(NeptuneScaleError):
    message = """
{h1}
----NeptuneRunConflicting------------------------------------------------------
{end}
Run with specified `run_id` already exists, but has different creation parameters (`family` or `fork_run_id`).

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneRunForkParentNotFound(NeptuneScaleWarning):
    message = """
{h1}
----NeptuneRunForkParentNotFound-----------------------------------------------
{end}
Missing fork parent run.

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneRunInvalidCreationParameters(NeptuneScaleError):
    message = """
{h1}
----NeptuneRunInvalidCreationParameters----------------------------------------
{end}
Invalid run creation parameters. For example, the experiment name is too large.

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneFieldPathExceedsSizeLimit(NeptuneScaleError):
    message = """
{h1}
----NeptuneFieldPathExceedsSizeLimit-------------------------------------------
{end}
Field path is too long. Maximum length is 1024 bytes (not characters).

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneFieldPathEmpty(NeptuneScaleError):
    message = """
{h1}
----NeptuneFieldPathEmpty------------------------------------------------------
{end}
Field path is empty.

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneFieldPathInvalid(NeptuneScaleError):
    message = """
{h1}
----NeptuneFieldPathInvalid----------------------------------------------------
{end}
Field path is invalid. To troubleshoot the problem, ensure that the UTF-8 encoding is valid.

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneFieldPathNonWritable(NeptuneScaleError):
    message = """
{h1}
----NeptuneFieldPathNonWritable------------------------------------------------
{end}
Field path is non-writable. Some special sys/ fields are read-only.

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneFieldTypeUnsupported(NeptuneScaleError):
    message = """
{h1}
----NeptuneFieldTypeUnsupported------------------------------------------------
{end}
Field type is not supported by the system.

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneFieldTypeConflicting(NeptuneScaleError):
    message = """
{h1}
----NeptuneFieldTypeConflicting------------------------------------------------
{end}
Field type is different from the one that was previously logged for this series.
Once a field type is set, it cannot be changed.

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneSeriesPointDuplicate(NeptuneScaleWarning):
    message = """
{h1}
----NeptuneSeriesPointDuplicate------------------------------------------------
{end}
The exact same data point was already logged for this series.

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneSeriesStepNonIncreasing(NeptuneScaleError):
    message = """
{h1}
----NeptuneSeriesStepNonIncreasing---------------------------------------------
{end}
The step of a series value is smaller than the most recently logged step for this series or the step is exactly the same,
  but the value is different.

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneSeriesStepNotAfterForkPoint(NeptuneScaleError):
    message = """
{h1}
----NeptuneSeriesStepNotAfterForkPoint-----------------------------------------
{end}
The series value must be greater than the step specified by the `fork_step` argument.

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneSeriesTimestampDecreasing(NeptuneScaleError):
    message = """
{h1}
----NeptuneSeriesTimestampDecreasing-------------------------------------------
{end}
The timestamp of a series value is less than the most recently logged value. Identical timestamps are allowed.

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneFloatValueNanInfUnsupported(NeptuneScaleError):
    message = """
{h1}
----NeptuneFloatValueNanInfUnsupported-----------------------------------------
{end}
Unsupported value type for float64 field or float64 series. Applies to Inf and NaN values.

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneStringValueExceedsSizeLimit(NeptuneScaleError):
    message = """
{h1}
----NeptuneStringValueExceedsSizeLimit-----------------------------------------
{end}
String value is too long. Maximum length is 64KB.

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneStringSetExceedsSizeLimit(NeptuneScaleError):
    message = """
{h1}
----NeptuneStringSetExceedsSizeLimit-------------------------------------------
{end}
String Set value is too long. Maximum length is 64KB.

{correct}Need help?{end}-> Contact support@neptune.ai

Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
"""


class NeptuneAsyncLagThresholdExceeded(NeptuneScaleError):
    message = """
    {h1}
    ----NeptuneAsyncLagThresholdExceeded----------------------------------------
    {end}
    Neptune is experiencing a high delay in synchronizing data.

    {correct}Need help?{end}-> Contact support@neptune.ai

    Struggling with the formatting? To disable it, set the `NEPTUNE_DISABLE_COLORS` environment variable to `True`.
    """
