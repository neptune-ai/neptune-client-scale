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

"""Contains shared errors types that can be raised from API functions"""

import httpx

__all__ = [
    "UnexpectedStatus",
    "InvalidApiTokenException",
    "UnableToExchangeApiKeyError",
    "ApiKeyRejectedError",
    "UnableToDeserializeApiKeyError",
    "UnableToParseResponse",
    "UnableToRefreshTokenError",
]


class UnexpectedStatus(Exception):
    """Raised by api functions when the response status an undocumented status and Client.raise_on_unexpected_status is True"""

    def __init__(self, status_code: int, content: bytes):
        self.status_code = status_code
        self.content = content

        super().__init__(
            f"Unexpected status code: {status_code}\n\nResponse content:\n{content.decode(errors='ignore')}"
        )


class InvalidApiTokenException(Exception):
    """Raised when the api token is invalid"""

    def __init__(self, reason: str = "") -> None:
        super().__init__(f"Invalid API token. Reason: {reason}")


class UnableToExchangeApiKeyError(Exception):
    """Raised when the API key exchange fails for any reason other than the API key being
    explicitly rejected by the server"""

    def __init__(self, reason: str = "Unknown") -> None:
        super().__init__(f"Unable to exchange API key. Reason: {reason}")


class ApiKeyRejectedError(Exception):
    """Raised when the backend rejects an API key because of it being unknown or expired"""

    def __init__(self) -> None:
        super().__init__("Your API key was rejected by the Neptune backend because it is either unknown or expired.")


class UnableToDeserializeApiKeyError(Exception):
    """Raised when the API key cannot be deserialized"""

    def __init__(self) -> None:
        super().__init__("Unable to deserialize API key")


class UnableToRefreshTokenError(Exception):
    """Raised when the token refresh fails"""

    def __init__(self, reason: str = "Unknown") -> None:
        super().__init__(f"Unable to refresh token. Reason: {reason}")


class UnableToParseResponse(Exception):
    """Raise when there is an exception during parsing a response"""

    def __init__(self, exception: BaseException, response: httpx.Response) -> None:
        self.exception = exception
        self.response = response
        super().__init__(
            f"Unable to parse server response: {exception}. "
            f"Response: HTTP {response.status_code}: {response.content!r}"
        )
