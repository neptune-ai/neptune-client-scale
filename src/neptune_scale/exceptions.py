from __future__ import annotations

__all__ = (
    "NeptuneScaleError",
    "NeptuneScaleWarning",
    "NeptuneUnableToLogData",
    "NeptuneUnauthorizedError",
    "NeptuneBadRequestError",
    "NeptuneInvalidCredentialsError",
    "NeptuneUnexpectedError",
    "NeptuneConnectionLostError",
    "NeptuneUnableToAuthenticateError",
    "NeptuneFileUploadError",
    "NeptuneFileUploadTemporaryError",
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
    "NeptuneAttributePathExceedsSizeLimit",
    "NeptuneAttributePathEmpty",
    "NeptuneAttributePathInvalid",
    "NeptuneAttributePathNonWritable",
    "NeptuneAttributeTypeUnsupported",
    "NeptuneAttributeTypeMismatch",
    "NeptuneSeriesPointDuplicate",
    "NeptuneSeriesStepNonIncreasing",
    "NeptuneSeriesStepNotAfterForkPoint",
    "NeptuneSeriesTimestampDecreasing",
    "NeptuneFloatValueNanInfUnsupported",
    "GenericFloatValueNanInfUnsupported",
    "NeptuneStringValueExceedsSizeLimit",
    "NeptuneStringSetExceedsSizeLimit",
    "NeptuneSynchronizationStopped",
    "NeptuneFileMetadataExceedsSizeLimit",
    "NeptuneProjectNotProvided",
    "NeptuneApiTokenNotProvided",
    "NeptuneTooManyRequestsResponseError",
    "NeptunePreviewStepNotAfterLastCommittedStep",
    "NeptuneDatabaseConflict",
    "NeptuneLocalStorageInUnsupportedVersion",
    "NeptuneHistogramBinEdgesContainNaN",
    "NeptuneHistogramTooManyBins",
    "NeptuneHistogramBinEdgesNotIncreasing",
    "NeptuneHistogramValuesLengthMismatch",
)

from typing import (
    Any,
    Optional,
)

from neptune_scale.util.styles import (
    STYLES,
    ensure_style_detected,
)


class NeptuneScaleError(Exception):
    message = "An error occurred in the Neptune client."

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ensure_style_detected()
        message = kwargs.pop("message", self.message)
        super().__init__(message.format(*args, **STYLES, **kwargs))


class NeptuneScaleWarning(Warning):
    message = "A warning occurred in the Neptune client."

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ensure_style_detected()
        super().__init__(self.message.format(*args, **STYLES, **kwargs))


class NeptuneBadRequestError(NeptuneScaleError):
    """
    A generic "bad request" error. Pass `reason` to provide a custom message.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        reason = kwargs.get("reason", None)
        if not reason:
            reason = "The request contains invalid data"
        kwargs["reason"] = reason
        kwargs["status_code"] = kwargs.get("status_code", 400)
        super().__init__(*args, **kwargs)

    message = """
{h1}
NeptuneBadRequestError({status_code}): {reason}
"""


class NeptuneSynchronizationStopped(NeptuneScaleError):
    message = """
{h1}
NeptuneSynchronizationStopped: The background synchronization process has stopped unexpectedly.
{end}

Your data is still being saved locally. You can manually synchronize it with the
Neptune backend later by running `neptune sync`.
"""


class NeptuneUnableToLogData(NeptuneScaleError):
    message = """
{h1}
NeptuneUnableToLogData: An error occurred, preventing Neptune from logging your data.
{end}
{reason}
"""

    def __init__(self, reason: str = "", **kwargs: Any) -> None:
        super().__init__(reason=reason, **kwargs)


class NeptuneFloatValueNanInfUnsupported(NeptuneUnableToLogData):
    message = """
{h1}
NeptuneFloatValueNanInfUnsupported: metric `{metric}` at step `{step}` has non-finite value of `{value}`.
{end}
Float series do not support logging NaN and Inf values. You can only log NaN and Inf as single config values.

You can configure Neptune to skip non-finite metric values by setting the `NEPTUNE_SKIP_NON_FINITE_METRICS`
environment variable to `True`.

For details, see https://docs.neptune.ai/log_configs
"""

    def __init__(self, metric: str = "", step: Optional[float | int] = None, value: Any = None) -> None:
        super().__init__(metric=metric, step=step, value=value)


class GenericFloatValueNanInfUnsupported(NeptuneFloatValueNanInfUnsupported):
    message = """
{h1}
NeptuneFloatValueNanInfUnsupported: Float series do not support logging NaN and Inf values. You can only log NaN and Inf as single config values.
{end}

You can configure Neptune to skip non-finite metric values by setting the `NEPTUNE_SKIP_NON_FINITE_METRICS`
environment variable to `True`.

For details, see https://docs.neptune.ai/log_configs
"""

    def __init__(self, *_args: Any) -> None:
        # Because this class defines its own message, the argument values to super().__init__ don't matter here.
        # However, we still need to accept *args, for unpickling to work properly. At the end of inheritance chain,
        # NeptuneScaleError calls Exception.__init__() with the `message` argument. This gets saved by pickle
        # and later used when unpickling the exception object, which would fail if __init__ didn't accept any
        # positional args. Our message is fixed, so this works OK.
        super().__init__(metric="", step=None, value=None)


class NeptuneUnauthorizedError(NeptuneScaleError):
    message = """
{h1}
NeptuneUnauthorizedError: You don't have permission to access the given resource.
{end}
    - Verify that your API token is correct. To find your API token:
        - Log in to Neptune and open the user menu.
        - If your workspace uses service accounts, ask the project owner to provide the token.

    - Verify that the provided project name is correct and the project exists.
      The correct project name should look like this: {correct}WORKSPACE_NAME/PROJECT_NAME{end}
      It has two parts:
          - {correct}WORKSPACE_NAME{end}: can be your username or your organization name
          - {correct}PROJECT_NAME{end}: the name specified for the project

   - Ask your workspace admin to grant you the necessary privileges to the project.
"""


class NeptuneInvalidCredentialsError(NeptuneScaleError):
    message = """
{h1}
NeptuneInvalidCredentialsError: The provided API token is invalid.
{end}

Make sure you copied your API token while logged in to Neptune.
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
For help, see https://docs.neptune.ai/setup
"""


class NeptuneUnexpectedError(NeptuneScaleError):
    message = """
{h1}
NeptuneUnexpectedError: An unexpected error occurred in the Neptune client.
{end}
Reason: `{reason}`

This is most likely a bug in the client. You can report it from our [Support Center](https://support.neptune.ai/).
"""

    def __init__(self, reason: str) -> None:
        super().__init__(reason=reason)


class NeptuneRetryableError(NeptuneScaleError):
    pass


class NeptuneConnectionLostError(NeptuneRetryableError):
    message = """
{h1}
NeptuneConnectionLostError: The connection to the Neptune server was lost.
{end}
Ensure that your computer is connected to the internet and that firewall settings aren't blocking the connection.
"""


class NeptuneUnexpectedResponseError(NeptuneRetryableError):
    message = """
{h1}
NeptuneUnexpectedResponseError: The Neptune server returned an unexpected response.
{end}
This is a temporary problem. If the problem persists, please report it from our [Support Center](https://support.neptune.ai/).
"""


class NeptuneTooManyRequestsResponseError(NeptuneRetryableError):
    message = """
{h1}
NeptuneTooManyRequestsResponseError: The Neptune server reported receiving too many requests.
{end}
This is a temporary problem. If the problem persists, please report it from our [Support Center](https://support.neptune.ai/).
"""


class NeptuneInternalServerError(NeptuneRetryableError):
    message = """
{h1}
NeptuneInternalServerError: We have encountered an internal server error.
{end}
This is a temporary problem. If the problem persists, please report it from our [Support Center](https://support.neptune.ai/).
"""


class NeptuneUnableToAuthenticateError(NeptuneRetryableError):
    message = """
{h1}
----NeptuneUnableToAuthenticateError: The client was unable to authenticate with the Neptune server.
{end}
Ensure that your API token is correct.
"""


class NeptuneFileUploadError(NeptuneScaleError):
    message = """
{h1}
----NeptuneFileUploadError: An unrecoverable error occurred during file upload
{end}
"""


class NeptuneFileUploadTemporaryError(NeptuneRetryableError):
    message = """
{h1}
----NeptuneFileUploadTemporaryError: A temporary error occurred during file upload
{end}
"""


class NeptuneProjectError(NeptuneScaleError):
    pass


class NeptuneProjectNotFound(NeptuneProjectError):
    message = """
{h1}
NeptuneProjectNotFound: Either the project hasn't been created yet or the name is incorrect.
{end}
For help, see https://docs.neptune.ai/setup
"""


class NeptuneProjectInvalidName(NeptuneProjectError):
    message = """
{h1}
NeptuneProjectInvalidName: Project name is either empty or too long.
{end}
"""


class NeptuneProjectAlreadyExists(NeptuneProjectError):
    message = """
{h1}
NeptuneProjectAlreadyExists: A project with the provided name or project key already exists.
{end}
"""


class NeptuneRunError(NeptuneScaleError):
    pass


class NeptuneRunNotFound(NeptuneRunError):
    message = """
{h1}
----NeptuneRunNotFound: Run not found.
{end}
"""


# This warning is less eye-popping by design as it may happen in perfectly valid scenarios.
class NeptuneRunDuplicate(NeptuneScaleWarning):
    message = (
        "NeptuneRunDuplicate: A run with the provided ID already exists. "
        "This is expected if you are resuming a run or using a distributed workflow."
    )


class NeptuneRunConflicting(NeptuneRunError):
    message = """
{h1}
NeptuneRunConflicting: Run with specified `run_id` already exists, but has a different `fork_run_id` parameter.
{end}
For forking instructions, see https://docs.neptune.ai/fork_experiment
"""


class NeptuneRunForkParentNotFound(NeptuneScaleWarning):
    message = """
{h1}
----NeptuneRunForkParentNotFound: The provided parent run does not exist.
{end}
For forking instructions, see https://docs.neptune.ai/fork_experiment
"""


class NeptuneRunInvalidCreationParameters(NeptuneRunError):
    message = """
{h1}
NeptuneRunInvalidCreationParameters: Run creation parameters rejected by the server.
{end}
For example, the experiment name is too large.
"""


class NeptuneAttributePathExceedsSizeLimit(NeptuneScaleError):
    message = """
{h1}
NeptuneAttributePathExceedsSizeLimit: Attribute name is too long.
{end}
The maximum length is 1024 bytes (not characters) in UTF-8 encoding.
"""


class NeptuneAttributePathEmpty(NeptuneScaleError):
    message = """
{h1}
NeptuneAttributePathEmpty: Attribute path is empty.
{end}
"""


class NeptuneAttributePathInvalid(NeptuneScaleError):
    message = """
{h1}
NeptuneAttributePathInvalid: Attribute path is invalid.
{end}
To troubleshoot the problem, ensure that the provided path correctly encodes to UTF-8.
"""


class NeptuneAttributePathNonWritable(NeptuneScaleError):
    message = """
{h1}
NeptuneAttributePathNonWritable: Attribute is not writable.
{end}
You could be trying to overwrite a read-only attribute. Note that most of the "sys/*" attributes are read-only.

For details, see https://docs.neptune.ai/sys
"""


class NeptuneAttributeTypeUnsupported(NeptuneScaleError):
    message = """
{h1}
NeptuneAttributeTypeUnsupported: the provided attribute type is not supported by Neptune.
{end}
For supported types, see https://docs.neptune.ai/attribute_types
"""


class NeptuneAttributeTypeMismatch(NeptuneScaleError):
    message = """
{h1}
NeptuneAttributeTypeMismatch: the attribute type is different from the one that was previously logged for this series.
{end}
Once an attribute type is set, it cannot be changed. Example: you can't log strings to an existing float series.
"""


class NeptuneSeriesPointDuplicate(NeptuneScaleWarning):
    message = """
{h1}
NeptuneSeriesPointDuplicate: The exact same data point (value + step pair) was already logged for this series.
{end}
For help, see https://docs.neptune.ai/log_metrics
"""


class NeptuneSeriesStepNonIncreasing(NeptuneScaleError):
    message = """
{h1}
NeptuneSeriesStepNonIncreasing: Subsequent steps of a series must be strictly increasing.
{end}
For help, see https://docs.neptune.ai/log_metrics
"""


class NeptuneSeriesStepNotAfterForkPoint(NeptuneScaleError):
    message = """
{h1}
NeptuneSeriesStepNotAfterForkPoint: The series value must be greater than the step specified by the `fork_step` argument.
{end}
For help, see https://docs.neptune.ai/fork_experiment
"""


class NeptuneSeriesTimestampDecreasing(NeptuneScaleError):
    message = """
{h1}
NeptuneSeriesTimestampDecreasing: The timestamp of a series value is less than the most recently logged value.
{end}
Existing timestamps are allowed. For help, see https://docs.neptune.ai/log_metrics
"""


class NeptuneStringValueExceedsSizeLimit(NeptuneScaleError):
    message = """
{h1}
NeptuneStringValueExceedsSizeLimit: String value is too long. Maximum length is 64KB.
{end}
"""


class NeptuneStringSetExceedsSizeLimit(NeptuneScaleError):
    message = """
{h1}
NeptuneStringSetExceedsSizeLimit: String Set value is too long. Maximum length is 64KB.
{end}
"""


class NeptuneFileMetadataExceedsSizeLimit(NeptuneScaleError):
    message = """
{h1}
NeptuneFileMetadataExceedsSizeLimit: File metadata is too long. Maximum length is 4KB.
{end}
"""


class NeptuneProjectNotProvided(NeptuneRetryableError):
    message = """
{h1}
NeptuneProjectNotProvided: The project name was not provided.
{end}
Make sure to specify the project name in the `project` parameter of the `Run`
constructor or with the `NEPTUNE_PROJECT` environment variable.

For instructions, see https://docs.neptune.ai/setup

"""


class NeptuneApiTokenNotProvided(NeptuneRetryableError):
    message = """
{h1}
NeptuneApiTokenNotProvided: The Neptune API token was not provided.
{end}
Make sure to specify the API token in the `api_token` parameter of the `Run`
constructor or with the `NEPTUNE_API_TOKEN` environment variable.

For instructions, see https://docs.neptune.ai/api_token
"""


class NeptunePreviewStepNotAfterLastCommittedStep(NeptuneScaleError):
    message = """
{h1}
NeptunePreviewStepNotAfterLastCommittedStep: Metric preview can only be logged
for steps greater than the last committed value.
{end}
It looks like you tried to log a preview (incomplete) metric update for a step that isn't after
the last fully committed (complete) update. Once a complete value is recorded, any preview updates
must only be added for later steps. Please adjust the order of your updates and try again.
"""


class NeptuneLocalStorageInUnsupportedVersion(NeptuneScaleError):
    message = """The local storage database is in an unsupported version.
    This may happen when you try to use a database created with a newer version of Neptune with an older version of the library.
    Please either upgrade Neptune to the latest version or create a new local storage database."""


class NeptuneDatabaseConflict(NeptuneScaleError):
    message = """NeptuneDatabaseConflict: Database with the same name `{name}` already exists."""

    def __init__(self, path: str = "") -> None:
        super().__init__(name=path)


class NeptuneHistogramBinEdgesContainNaN(NeptuneScaleError):
    message = """
{h1}
NeptuneHistogramBinEdgesContainNaN: Histogram bin edges cannot contain NaN values.
{end}
"""


class NeptuneHistogramTooManyBins(NeptuneScaleError):
    message = """
{h1}
NeptuneHistogramTooManyBins: Histogram can have at most 512 bins
{end}
This corresponds to the maximum number of 513 bin edges.
"""


class NeptuneHistogramBinEdgesNotIncreasing(NeptuneScaleError):
    message = """
{h1}
NeptuneHistogramBinEdgesNotIncreasing: Histogram bin edges must be strictly increasing.
{end}
"""


class NeptuneHistogramValuesLengthMismatch(NeptuneScaleError):
    message = """
{h1}
NeptuneHistogramValuesLengthMismatch: Histogram counts/densities length must be equal to bin edges length - 1.
{end}
"""
