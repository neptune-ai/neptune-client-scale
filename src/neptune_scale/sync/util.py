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

import os
import signal
from typing import Optional

from neptune_scale.exceptions import NeptuneApiTokenNotProvided
from neptune_scale.util.envs import API_TOKEN_ENV_NAME


def safe_signal_name(signum: int) -> str:
    try:
        signame = signal.Signals(signum).name
    except ValueError:
        signame = str(signum)

    return signame


def ensure_api_token(api_token: Optional[str]) -> str:
    """Ensure the API token is provided via either explicit argument, or env variable."""

    api_token = api_token or os.environ.get(API_TOKEN_ENV_NAME)
    if api_token is None:
        raise NeptuneApiTokenNotProvided()

    return api_token
