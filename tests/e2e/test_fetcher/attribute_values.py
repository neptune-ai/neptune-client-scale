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
import functools as ft
from typing import (
    Any,
    Generator,
    Iterable,
    Optional,
)

from neptune_api.client import AuthenticatedClient
from neptune_retrieval_api.api.default import query_attributes_within_project_proto
from neptune_retrieval_api.models import QueryAttributesBodyDTO
from neptune_retrieval_api.proto.neptune_pb.api.v1.model.attributes_pb2 import ProtoQueryAttributesResultDTO

from tests.e2e.test_fetcher import identifiers, paging


def fetch_attribute_values(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    run_identifiers: Iterable[identifiers.RunIdentifier],
    attribute_definitions: Iterable[identifiers.AttributeDefinition],
) -> Generator[util.Page[AttributeValue], None, None]:
    attribute_definitions_set: set[AttributeDefinition] = set(attribute_definitions)
    experiments = [str(e) for e in run_identifiers]

    if not attribute_definitions_set or not run_identifiers:
        yield from []
        return

    params: dict[str, Any] = {
        "experimentIdsFilter": experiments,
        "attributeNamesFilter": [ad.name for ad in attribute_definitions],
        "nextPage": {"limit": batch_size},
    }

    yield from paging.fetch_pages(
        client=client,
        fetch_page=ft.partial(_fetch_attribute_values_page, project_identifier=project_identifier),
        process_page=ft.partial(
            _process_attribute_values_page,
            attribute_definitions_set=attribute_definitions_set,
            project_identifier=project_identifier,
        ),
        make_new_page_params=_make_new_attribute_values_page_params,
        params=params,
    )


def _fetch_attribute_values_page(
    client: AuthenticatedClient,
    params: dict[str, Any],
    project_identifier: identifiers.ProjectIdentifier,
) -> ProtoQueryAttributesResultDTO:
    response = util.backoff_retry(
        query_attributes_within_project_proto.sync_detailed,
        client=client,
        body=QueryAttributesBodyDTO.from_dict(params),
        project_identifier=project_identifier,
    )
    return ProtoQueryAttributesResultDTO.FromString(response.content)


def _process_attribute_values_page(
    data: ProtoQueryAttributesResultDTO,
    attribute_definitions_set: set[AttributeDefinition],
    project_identifier: identifiers.ProjectIdentifier,
) -> util.Page[AttributeValue]:
    items = []
    for entry in data.entries:
        run_identifier = identifiers.RunIdentifier(
            project_identifier=project_identifier, sys_id=identifiers.SysId(entry.experimentShortId)
        )

        for attr in entry.attributes:
            attr_definition = AttributeDefinition(name=attr.name, type=map_attribute_type_backend_to_python(attr.type))
            if attr_definition not in attribute_definitions_set:
                continue

            item_value = extract_value(attr)
            if item_value is None:
                continue

            attr_value = AttributeValue(
                attribute_definition=attr_definition,
                value=item_value,
                run_identifier=run_identifier,
            )
            items.append(attr_value)

    return util.Page(items=items)


def _make_new_attribute_values_page_params(
    params: dict[str, Any], data: Optional[ProtoQueryAttributesResultDTO]
) -> Optional[dict[str, Any]]:
    if data is None:
        if "nextPageToken" in params["nextPage"]:
            del params["nextPage"]["nextPageToken"]
        return params

    next_page_token = data.nextPage.nextPageToken
    if not next_page_token:
        return None

    params["nextPage"]["nextPageToken"] = next_page_token
    return params
