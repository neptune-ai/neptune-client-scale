from __future__ import annotations

from neptune_scale.exceptions import (
    NeptuneConnectionLostError,
    NeptuneInternalServerError,
    NeptuneTooManyRequestsResponseError,
    NeptuneUnauthorizedError,
    NeptuneUnexpectedResponseError,
)
from neptune_scale.util import get_logger

logger = get_logger()


def raise_for_http_status(status_code: int) -> None:
    assert status_code >= 400, f"Status code {status_code} is not an error"

    logger.error("HTTP response error: %s", status_code)
    if status_code == 403:
        raise NeptuneUnauthorizedError()
    elif status_code == 408:
        raise NeptuneConnectionLostError()
    elif status_code == 429:
        raise NeptuneTooManyRequestsResponseError()
    elif status_code // 100 == 5:
        raise NeptuneInternalServerError()
    else:
        raise NeptuneUnexpectedResponseError()


def escape_nql_criterion(criterion: str) -> str:
    """
    Escape backslash and (double-)quotes in the string, to match what the NQL engine expects.
    """

    return criterion.replace("\\", r"\\").replace('"', r"\"")
