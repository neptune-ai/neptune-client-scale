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
from collections.abc import Iterable
from typing import (
    Any,
    Optional,
    Union,
)

from neptune_api.api.retrieval import get_multiple_float_series_values_proto
from neptune_api.client import AuthenticatedClient
from neptune_api.models import FloatTimeSeriesValuesRequest
from neptune_api.proto.neptune_pb.api.v1.model.series_values_pb2 import ProtoFloatSeriesValuesResponseDTO

from . import (
    fetch_attribute_values,
    identifiers,
)

_TOTAL_POINT_LIMIT: int = 1_000_000


def fetch_metric_values(
    client: AuthenticatedClient,
    project: identifiers.ProjectIdentifier,
    *,
    attributes: Iterable[identifiers.AttributePath],
    custom_run_id: Optional[identifiers.CustomRunId] = None,
    run_id: Optional[identifiers.SysId] = None,
    step_range: tuple[Union[float, None], Union[float, None]] = (None, None),
) -> dict[identifiers.AttributePath, dict[float, float]]:
    attribute_set = set(attributes)

    if not attribute_set:
        return {}

    if run_id is not None:
        holder_identifier = f"{project}/{run_id}"
    elif custom_run_id is not None:
        # CUSTOM/{project}/{custom_run_id} does not work for some reason
        sys_attrs = fetch_attribute_values(
            client=client, project=project, custom_run_id=custom_run_id, attributes=["sys/id"]
        )
        run_id = sys_attrs["sys/id"]
        holder_identifier = f"{project}/{run_id}"
    else:
        raise ValueError("Either run_id or custom_run_id must be provided")

    request_id_to_attribute: dict[str, identifiers.AttributePath] = {
        f"{i}": attr for i, attr in enumerate(attribute_set)
    }

    params: dict[str, Any] = {
        "requests": [
            {
                "requestId": request_id,
                "series": {
                    "holder": {
                        "identifier": holder_identifier,
                        "type": "experiment",
                    },
                    "attribute": attribute,
                    "lineage": "FULL",
                },
            }
            for request_id, attribute in request_id_to_attribute.items()
        ],
        "stepRange": {"from": step_range[0], "to": step_range[1]},
        "order": "ascending",
        "perSeriesPointsLimit": _TOTAL_POINT_LIMIT / len(attribute_set),
    }

    response = _fetch_metrics(client, params)
    return _process_metrics_response(response, request_id_to_attribute)


def _fetch_metrics(
    client: AuthenticatedClient,
    params: dict[str, Any],
) -> ProtoFloatSeriesValuesResponseDTO:
    body = FloatTimeSeriesValuesRequest.from_dict(params)

    response = get_multiple_float_series_values_proto.sync_detailed(client=client, body=body)

    return ProtoFloatSeriesValuesResponseDTO.FromString(response.content)


def _process_metrics_response(
    data: ProtoFloatSeriesValuesResponseDTO,
    request_id_to_attribute: dict[str, identifiers.AttributePath],
) -> dict[identifiers.AttributePath, dict[float, float]]:
    items: dict[identifiers.AttributePath, dict[float, float]] = {}

    for series in data.series:
        if series.series.values:
            attribute = request_id_to_attribute[series.requestId]
            values = {float(value.step): float(value.value) for value in series.series.values}
            items.setdefault(attribute, {}).update(values)

    return items
