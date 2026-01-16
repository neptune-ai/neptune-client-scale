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

from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Type,
    TypeVar,
    Union,
)

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import (
    UNSET,
    Unset,
)

if TYPE_CHECKING:
    from ..models.query_leaderboard_params_pagination_dto import QueryLeaderboardParamsPaginationDTO


T = TypeVar("T", bound="QueryLeaderboardParamsPaginationWithContinuationTokenDTO")


@_attrs_define
class QueryLeaderboardParamsPaginationWithContinuationTokenDTO:
    """
    Attributes:
        pagination (QueryLeaderboardParamsPaginationDTO):
        continuation_token (Union[Unset, str]):
        before_token (Union[Unset, str]):
    """

    pagination: "QueryLeaderboardParamsPaginationDTO"
    continuation_token: Union[Unset, str] = UNSET
    before_token: Union[Unset, str] = UNSET
    additional_properties: Dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        pagination = self.pagination.to_dict()

        continuation_token = self.continuation_token

        before_token = self.before_token

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "pagination": pagination,
            }
        )
        if continuation_token is not UNSET:
            field_dict["continuationToken"] = continuation_token
        if before_token is not UNSET:
            field_dict["beforeToken"] = before_token

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.query_leaderboard_params_pagination_dto import QueryLeaderboardParamsPaginationDTO

        d = src_dict.copy()
        pagination = QueryLeaderboardParamsPaginationDTO.from_dict(d.pop("pagination"))

        continuation_token = d.pop("continuationToken", UNSET)

        before_token = d.pop("beforeToken", UNSET)

        query_leaderboard_params_pagination_with_continuation_token_dto = cls(
            pagination=pagination,
            continuation_token=continuation_token,
            before_token=before_token,
        )

        query_leaderboard_params_pagination_with_continuation_token_dto.additional_properties = d
        return query_leaderboard_params_pagination_with_continuation_token_dto

    @property
    def additional_keys(self) -> List[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
