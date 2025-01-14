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

from typing import Optional

from neptune_retrieval_api.models import SearchLeaderboardEntriesParamsDTO

from neptune_scale.exceptions import NeptuneScaleError
from neptune_scale.net.api_client import HostedApiClient
from neptune_scale.net.util import escape_nql_criterion
from neptune_scale.sync.util import ensure_api_token


def run_exists(project: str, run_id: str, api_token: Optional[str] = None) -> bool:
    """Query the backend for the existence of a Run with the given ID.

    Returns True if the Run exists, False otherwise.
    """

    client = HostedApiClient(api_token=ensure_api_token(api_token))
    body = SearchLeaderboardEntriesParamsDTO.from_dict(
        {
            "query": {"query": f'`sys/custom_run_id`:string = "{escape_nql_criterion(run_id)}"'},
        }
    )

    try:
        result = client.search_entries(project, body)
    except Exception as e:
        raise NeptuneScaleError(reason=e)

    return bool(result.entries)
