#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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

from __future__ import annotations

import logging
from collections.abc import Generator
from typing import (
    Any,
    Callable,
    Optional,
    TypeVar,
)

from neptune_api import AuthenticatedClient

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")
_Params = dict[str, Any]


def fetch_pages(
    client: AuthenticatedClient,
    fetch_page: Callable[[AuthenticatedClient, _Params], R],
    process_page: Callable[[R], T],
    make_new_page_params: Callable[[_Params, Optional[R]], Optional[_Params]],
    params: _Params,
) -> Generator[T, None, None]:
    page_params = make_new_page_params(params, None)
    while page_params is not None:
        data = fetch_page(client, page_params)
        page = process_page(data)
        page_params = make_new_page_params(page_params, data)
        yield page
