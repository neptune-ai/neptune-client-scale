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

PROJECT_ENV_NAME = "NEPTUNE_PROJECT"

API_TOKEN_ENV_NAME = "NEPTUNE_API_TOKEN"

DISABLE_COLORS = "NEPTUNE_DISABLE_COLORS"

DEBUG_MODE = "NEPTUNE_DEBUG_MODE"

SUBPROCESS_KILL_TIMEOUT = "NEPTUNE_SUBPROCESS_KILL_TIMEOUT"

ALLOW_SELF_SIGNED_CERTIFICATE = "NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE"
SKIP_NON_FINITE_METRICS = "NEPTUNE_SKIP_NON_FINITE_METRICS"


def get_bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).lower() in ("true", "1")
