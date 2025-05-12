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
import datetime
from collections.abc import Iterable
from typing import (
    Any,
    Optional,
)

from neptune_api.client import AuthenticatedClient
from neptune_retrieval_api.api.default import query_attributes_within_project_proto
from neptune_retrieval_api.models import QueryAttributesBodyDTO
from neptune_retrieval_api.proto.neptune_pb.api.v1.model.attributes_pb2 import ProtoQueryAttributesResultDTO
from neptune_retrieval_api.proto.neptune_pb.api.v1.model.leaderboard_entries_pb2 import ProtoAttributeDTO

from . import identifiers


def fetch_attribute_values(
    client: AuthenticatedClient,
    project: identifiers.ProjectIdentifier,
    *,
    attributes: Iterable[identifiers.AttributePath],
    custom_run_id: Optional[identifiers.CustomRunId] = None,
    run_id: Optional[identifiers.SysId] = None,
) -> dict[identifiers.AttributePath, Any]:
    attribute_set: set[identifiers.AttributePath] = set(attributes)

    if not attribute_set:
        return {}

    if custom_run_id is not None:
        experiment_id = f"CUSTOM/{project}/{custom_run_id}"
    elif run_id is not None:
        experiment_id = f"{project}/{run_id}"
    else:
        raise ValueError("Either custom_run_id or run_id must be provided")

    params: dict[str, Any] = {
        "experimentIdsFilter": [experiment_id],
        "attributeNamesFilter": list(attribute_set),
        "nextPage": {"limit": 10_000},
    }

    response = _fetch_attribute_values(client, params, project)
    result = _process_attribute_values_response(response)

    assert len(result) == 1, f"Expected one run in the result, got {len(result)}"
    return next(iter(result.values()))


def _fetch_attribute_values(
    client: AuthenticatedClient,
    params: dict[str, Any],
    project: identifiers.ProjectIdentifier,
) -> ProtoQueryAttributesResultDTO:
    body = QueryAttributesBodyDTO.from_dict(params)
    response = query_attributes_within_project_proto.sync_detailed(
        client=client,
        body=body,
        project_identifier=project,
    )
    return ProtoQueryAttributesResultDTO.FromString(response.content)


def _process_attribute_values_response(
    data: ProtoQueryAttributesResultDTO,
) -> dict[str, dict[identifiers.AttributePath, Any]]:
    result: dict[str, dict[identifiers.AttributePath, Any]] = {}

    for entry in data.entries:
        sys_id = entry.experimentShortId

        for attr in entry.attributes:
            item_value = _extract_value(attr)
            if item_value is None:
                continue

            sys_id_dict = result.setdefault(sys_id, {})
            sys_id_dict[attr.name] = item_value

    return result


def _extract_value(attr: ProtoAttributeDTO) -> Optional[Any]:
    if attr.type == "string":
        return attr.string_properties.value
    elif attr.type == "int":
        return attr.int_properties.value
    elif attr.type == "float":
        return attr.float_properties.value
    elif attr.type == "bool":
        return attr.bool_properties.value
    elif attr.type == "datetime":
        return datetime.datetime.fromtimestamp(attr.datetime_properties.value / 1000, tz=datetime.timezone.utc)
    elif attr.type == "stringSet":
        return set(attr.string_set_properties.value)
    elif attr.type == "floatSeries":
        properties = attr.float_series_properties
        return dict(
            last=properties.last,
            min=properties.min,
            max=properties.max,
            average=properties.average,
            variance=properties.variance,
        )
    elif attr.type == "stringSeries":
        properties = attr.string_series_properties
        return dict(
            last=properties.last,
            last_step=properties.last_step,
        )
    elif attr.type == "fileRef":
        properties = attr.file_ref_properties
        return dict(
            path=properties.path,
            size_bytes=properties.sizeBytes,
            mime_type=properties.mimeType,
        )
    elif attr.type == "experimentState":
        return None
    else:
        raise NotImplementedError(f"Unsupported attribute type: {attr.type}")
