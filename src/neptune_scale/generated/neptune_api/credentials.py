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

__all__ = ["Credentials"]

import base64
import json
from typing import (
    Any,
    Dict,
)

from attr import define

from .errors import (
    InvalidApiTokenException,
    UnableToDeserializeApiKeyError,
)


@define
class Credentials:
    api_key: str
    base_url: str

    @classmethod
    def from_api_key(cls, api_key: str) -> "Credentials":
        api_key = api_key.strip()

        try:
            token_data = deserialize(api_key)
        except UnableToDeserializeApiKeyError as e:
            raise InvalidApiTokenException("Unable to deserialize API key") from e

        token_origin_address, api_url = token_data.get("api_address"), token_data.get("api_url")
        if not token_origin_address and not api_url:
            raise InvalidApiTokenException("API key is missing required fields")

        return Credentials(api_key=api_key, base_url=str(api_url) or str(token_origin_address))


def deserialize(api_key: str) -> Dict[str, Any]:
    try:
        data: Dict[str, Any] = json.loads(base64.b64decode(api_key.encode()).decode("utf-8"))
        return data
    except Exception as e:
        raise UnableToDeserializeApiKeyError() from e
