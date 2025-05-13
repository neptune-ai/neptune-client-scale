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
import pathlib
from collections.abc import Iterable
from typing import (
    Literal,
    Optional,
)

from azure.storage.blob import BlobClient
from neptune_api.client import AuthenticatedClient
from neptune_storage_api.api import storagebridge
from neptune_storage_api.models import (
    CreateSignedUrlsRequest,
    CreateSignedUrlsResponse,
    FileToSign,
    Permission,
)

from . import (
    fetch_attribute_values,
    fetch_series_values,
    identifiers,
)


def fetch_files(
    client: AuthenticatedClient,
    project: identifiers.ProjectIdentifier,
    *,
    attributes_targets: dict[identifiers.AttributePath, pathlib.Path],
    custom_run_id: Optional[identifiers.CustomRunId] = None,
    run_id: Optional[identifiers.SysId] = None,
) -> None:
    file_refs = fetch_attribute_values(
        client=client,
        project=project,
        custom_run_id=custom_run_id,
        run_id=run_id,
        attributes=list(attributes_targets.keys()),
    )

    source_targets = {file_ref["path"]: attributes_targets[attribute] for attribute, file_ref in file_refs.items()}

    if not source_targets:
        return

    _download_from_paths(
        client=client,
        project=project,
        source_targets=source_targets.items(),
    )


def fetch_file_series(
    client: AuthenticatedClient,
    project: identifiers.ProjectIdentifier,
    *,
    attributes_targets: dict[identifiers.AttributePath, pathlib.Path],
    custom_run_id: Optional[identifiers.CustomRunId] = None,
    run_id: Optional[identifiers.SysId] = None,
) -> None:
    file_ref_series = fetch_series_values(
        client=client,
        project=project,
        custom_run_id=custom_run_id,
        run_id=run_id,
        attributes=list(attributes_targets.keys()),
    )

    source_targets: dict[str, pathlib.Path] = {
        file_ref["path"]: attributes_targets[attribute] / f"{step:19.6f}"
        for attribute, series in file_ref_series.items()
        for step, file_ref in series.items()
    }

    if not source_targets:
        return

    _download_from_paths(
        client=client,
        project=project,
        source_targets=source_targets.items(),
    )


def _download_from_paths(
    client: AuthenticatedClient,
    project: identifiers.ProjectIdentifier,
    source_targets: Iterable[tuple[str, pathlib.Path]],
) -> None:
    signed_urls = _fetch_signed_urls(
        client=client,
        project=project,
        file_paths=[source_path for source_path, _ in source_targets],
    )

    for (_, target_path), url in zip(source_targets, signed_urls):
        _download_from_url(
            signed_url=url,
            target_path=target_path,
        )


def _fetch_signed_urls(
    client: AuthenticatedClient,
    project: identifiers.ProjectIdentifier,
    file_paths: Iterable[str],
    permission: Literal["read", "write"] = "read",
) -> list[str]:
    body = CreateSignedUrlsRequest(
        files=[
            FileToSign(project_identifier=project, path=path, permission=Permission(permission)) for path in file_paths
        ]
    )

    response = storagebridge.signed_url.sync_detailed(client=client, body=body)

    data: CreateSignedUrlsResponse = response.parsed

    return [file.url for file in data.files]


def _download_from_url(
    signed_url: str,
    target_path: pathlib.Path,
) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with open(target_path, mode="wb") as opened:
        blob_client = BlobClient.from_blob_url(signed_url)
        download_stream = blob_client.download_blob()
        for chunk in download_stream.chunks():
            opened.write(chunk)
