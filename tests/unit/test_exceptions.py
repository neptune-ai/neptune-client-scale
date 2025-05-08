import pickle

import pytest

from neptune_scale.exceptions import (
    GenericFloatValueNanInfUnsupported,
    NeptuneApiTokenNotProvided,
    NeptuneAsyncLagThresholdExceeded,
    NeptuneAttributePathEmpty,
    NeptuneAttributePathExceedsSizeLimit,
    NeptuneAttributePathInvalid,
    NeptuneAttributePathNonWritable,
    NeptuneAttributeTypeMismatch,
    NeptuneAttributeTypeUnsupported,
    NeptuneBadRequestError,
    NeptuneConnectionLostError,
    NeptuneDatabaseConflict,
    NeptuneFileMetadataExceedsSizeLimit,
    NeptuneFileUploadError,
    NeptuneFileUploadTemporaryError,
    NeptuneFloatValueNanInfUnsupported,
    NeptuneHistogramBinEdgesContainNaN,
    NeptuneHistogramBinEdgesNotIncreasing,
    NeptuneHistogramTooManyBins,
    NeptuneHistogramValuesLengthMismatch,
    NeptuneInternalServerError,
    NeptuneInvalidCredentialsError,
    NeptuneLocalStorageInUnsupportedVersion,
    NeptuneOperationsQueueMaxSizeExceeded,
    NeptunePreviewStepNotAfterLastCommittedStep,
    NeptuneProjectAlreadyExists,
    NeptuneProjectError,
    NeptuneProjectInvalidName,
    NeptuneProjectNotFound,
    NeptuneProjectNotProvided,
    NeptuneRetryableError,
    NeptuneRunConflicting,
    NeptuneRunError,
    NeptuneRunInvalidCreationParameters,
    NeptuneRunNotFound,
    NeptuneScaleError,
    NeptuneSeriesStepNonIncreasing,
    NeptuneSeriesStepNotAfterForkPoint,
    NeptuneSeriesTimestampDecreasing,
    NeptuneStringSetExceedsSizeLimit,
    NeptuneStringValueExceedsSizeLimit,
    NeptuneSynchronizationStopped,
    NeptuneTooManyRequestsResponseError,
    NeptuneUnableToAuthenticateError,
    NeptuneUnableToLogData,
    NeptuneUnauthorizedError,
    NeptuneUnexpectedError,
    NeptuneUnexpectedResponseError,
)

# These instances will be pickled and unpickled during test_all_exceptions_are_pickable().
#
# If there is an exception that inherits from NeptuneScaleError and is not in this list,
# test_all_exceptions_are_tested() will fail
EXCEPTIONS = (
    GenericFloatValueNanInfUnsupported(),
    NeptuneApiTokenNotProvided(),
    NeptuneAsyncLagThresholdExceeded(),
    NeptuneAttributePathEmpty(),
    NeptuneAttributePathExceedsSizeLimit(),
    NeptuneAttributePathInvalid(),
    NeptuneAttributePathNonWritable(),
    NeptuneAttributeTypeMismatch(),
    NeptuneAttributeTypeUnsupported(),
    NeptuneBadRequestError(),
    NeptuneConnectionLostError(),
    NeptuneDatabaseConflict("foo"),
    NeptuneFileMetadataExceedsSizeLimit(),
    NeptuneFileUploadError(),
    NeptuneFileUploadTemporaryError(),
    NeptuneFloatValueNanInfUnsupported(metric="foo", step=1, value=1),
    NeptuneInternalServerError(),
    NeptuneInvalidCredentialsError(),
    NeptuneLocalStorageInUnsupportedVersion(),
    NeptuneOperationsQueueMaxSizeExceeded(),
    NeptunePreviewStepNotAfterLastCommittedStep(),
    NeptuneProjectAlreadyExists(),
    NeptuneProjectError(),
    NeptuneProjectInvalidName(),
    NeptuneProjectNotFound(),
    NeptuneProjectNotProvided(),
    NeptuneRetryableError(),
    NeptuneRunConflicting(),
    NeptuneRunError(),
    NeptuneRunInvalidCreationParameters(),
    NeptuneRunNotFound(),
    NeptuneSeriesStepNonIncreasing(),
    NeptuneSeriesStepNotAfterForkPoint(),
    NeptuneSeriesTimestampDecreasing(),
    NeptuneStringSetExceedsSizeLimit(),
    NeptuneStringValueExceedsSizeLimit(),
    NeptuneSynchronizationStopped(),
    NeptuneTooManyRequestsResponseError(),
    NeptuneUnableToAuthenticateError(),
    NeptuneUnableToLogData(),
    NeptuneUnauthorizedError(),
    NeptuneUnexpectedError(reason="test"),
    NeptuneUnexpectedResponseError(),
    NeptuneHistogramBinEdgesContainNaN(),
    NeptuneHistogramTooManyBins(),
    NeptuneHistogramBinEdgesNotIncreasing(),
    NeptuneHistogramValuesLengthMismatch(),
)


@pytest.mark.parametrize("instance", EXCEPTIONS, ids=lambda e: type(e).__name__)
def test_all_exceptions_are_pickable(instance):
    """Test that all exceptions are pickable."""

    try:
        pickled_exception = pickle.dumps(instance)
        unpickled_exception = pickle.loads(pickled_exception)
        assert isinstance(unpickled_exception, type(instance))
    except Exception as e:
        pytest.fail(f"Exception {type(instance)} is not pickable: {e}")


def _all_subclasses(cls):
    """Recursively find all subclasses of a given class."""
    subclasses = set(cls.__subclasses__())

    for subclass in cls.__subclasses__():
        subclasses.update(_all_subclasses(subclass))

    return subclasses


def test_all_exceptions_are_tested():
    all_types = _all_subclasses(NeptuneScaleError)
    tested_types = set(type(e) for e in EXCEPTIONS)
    missing_types = all_types - tested_types

    if missing_types:
        pytest.fail("The following exceptions are not tested: " + ", ".join(t.__name__ for t in missing_types))
