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
    cast,
)

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import (
    UNSET,
    Unset,
)

if TYPE_CHECKING:
    from ..models.query_leaderboard_params_field_dto import QueryLeaderboardParamsFieldDTO
    from ..models.query_leaderboard_params_opened_group_with_pagination_params_dto import (
        QueryLeaderboardParamsOpenedGroupWithPaginationParamsDTO,
    )


T = TypeVar("T", bound="QueryLeaderboardParamsGroupingParamsDTO")


@_attrs_define
class QueryLeaderboardParamsGroupingParamsDTO:
    """
    Attributes:
        group_by (List['QueryLeaderboardParamsFieldDTO']):
        opened_groups (Union[Unset, List[str]]):
        opened_groups_with_pagination (Union[Unset, List['QueryLeaderboardParamsOpenedGroupWithPaginationParamsDTO']]):
    """

    group_by: List["QueryLeaderboardParamsFieldDTO"]
    opened_groups: Union[Unset, List[str]] = UNSET
    opened_groups_with_pagination: Union[Unset, List["QueryLeaderboardParamsOpenedGroupWithPaginationParamsDTO"]] = (
        UNSET
    )
    additional_properties: Dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        group_by = []
        for group_by_item_data in self.group_by:
            group_by_item = group_by_item_data.to_dict()
            group_by.append(group_by_item)

        opened_groups: Union[Unset, List[str]] = UNSET
        if not isinstance(self.opened_groups, Unset):
            opened_groups = self.opened_groups

        opened_groups_with_pagination: Union[Unset, List[Dict[str, Any]]] = UNSET
        if not isinstance(self.opened_groups_with_pagination, Unset):
            opened_groups_with_pagination = []
            for opened_groups_with_pagination_item_data in self.opened_groups_with_pagination:
                opened_groups_with_pagination_item = opened_groups_with_pagination_item_data.to_dict()
                opened_groups_with_pagination.append(opened_groups_with_pagination_item)

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "groupBy": group_by,
            }
        )
        if opened_groups is not UNSET:
            field_dict["openedGroups"] = opened_groups
        if opened_groups_with_pagination is not UNSET:
            field_dict["openedGroupsWithPagination"] = opened_groups_with_pagination

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.query_leaderboard_params_field_dto import QueryLeaderboardParamsFieldDTO
        from ..models.query_leaderboard_params_opened_group_with_pagination_params_dto import (
            QueryLeaderboardParamsOpenedGroupWithPaginationParamsDTO,
        )

        d = src_dict.copy()
        group_by = []
        _group_by = d.pop("groupBy")
        for group_by_item_data in _group_by:
            group_by_item = QueryLeaderboardParamsFieldDTO.from_dict(group_by_item_data)

            group_by.append(group_by_item)

        opened_groups = cast(List[str], d.pop("openedGroups", UNSET))

        opened_groups_with_pagination: Union[Unset, List[QueryLeaderboardParamsOpenedGroupWithPaginationParamsDTO]] = (
            UNSET
        )
        _opened_groups_with_pagination = d.pop("openedGroupsWithPagination", UNSET)
        if not isinstance(_opened_groups_with_pagination, Unset):
            opened_groups_with_pagination = []
            for opened_groups_with_pagination_item_data in _opened_groups_with_pagination:
                opened_groups_with_pagination_item = QueryLeaderboardParamsOpenedGroupWithPaginationParamsDTO.from_dict(
                    opened_groups_with_pagination_item_data
                )

                opened_groups_with_pagination.append(opened_groups_with_pagination_item)

        query_leaderboard_params_grouping_params_dto = cls(
            group_by=group_by,
            opened_groups=opened_groups,
            opened_groups_with_pagination=opened_groups_with_pagination,
        )

        query_leaderboard_params_grouping_params_dto.additional_properties = d
        return query_leaderboard_params_grouping_params_dto

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
