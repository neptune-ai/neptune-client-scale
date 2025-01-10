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
