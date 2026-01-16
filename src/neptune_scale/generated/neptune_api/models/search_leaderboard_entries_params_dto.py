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
    from ..models.nql_query_params_dto import NqlQueryParamsDTO
    from ..models.query_leaderboard_params_attribute_filter_dto import QueryLeaderboardParamsAttributeFilterDTO
    from ..models.query_leaderboard_params_grouping_params_dto import QueryLeaderboardParamsGroupingParamsDTO
    from ..models.query_leaderboard_params_pagination_dto import QueryLeaderboardParamsPaginationDTO
    from ..models.query_leaderboard_params_query_aliases_dto import QueryLeaderboardParamsQueryAliasesDTO
    from ..models.query_leaderboard_params_sorting_params_dto import QueryLeaderboardParamsSortingParamsDTO


T = TypeVar("T", bound="SearchLeaderboardEntriesParamsDTO")


@_attrs_define
class SearchLeaderboardEntriesParamsDTO:
    """
    Attributes:
        attribute_filters (Union[Unset, List['QueryLeaderboardParamsAttributeFilterDTO']]):
        experiment_leader (Union[Unset, bool]):
        grouping (Union[Unset, QueryLeaderboardParamsGroupingParamsDTO]):
        pagination (Union[Unset, QueryLeaderboardParamsPaginationDTO]):
        query (Union[Unset, NqlQueryParamsDTO]):
        query_name_aliases (Union[Unset, QueryLeaderboardParamsQueryAliasesDTO]):
        sorting (Union[Unset, QueryLeaderboardParamsSortingParamsDTO]):
        truncate_string_to (Union[Unset, int]):
    """

    attribute_filters: Union[Unset, List["QueryLeaderboardParamsAttributeFilterDTO"]] = UNSET
    experiment_leader: Union[Unset, bool] = UNSET
    grouping: Union[Unset, "QueryLeaderboardParamsGroupingParamsDTO"] = UNSET
    pagination: Union[Unset, "QueryLeaderboardParamsPaginationDTO"] = UNSET
    query: Union[Unset, "NqlQueryParamsDTO"] = UNSET
    query_name_aliases: Union[Unset, "QueryLeaderboardParamsQueryAliasesDTO"] = UNSET
    sorting: Union[Unset, "QueryLeaderboardParamsSortingParamsDTO"] = UNSET
    truncate_string_to: Union[Unset, int] = UNSET
    additional_properties: Dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        attribute_filters: Union[Unset, List[Dict[str, Any]]] = UNSET
        if not isinstance(self.attribute_filters, Unset):
            attribute_filters = []
            for attribute_filters_item_data in self.attribute_filters:
                attribute_filters_item = attribute_filters_item_data.to_dict()
                attribute_filters.append(attribute_filters_item)

        experiment_leader = self.experiment_leader

        grouping: Union[Unset, Dict[str, Any]] = UNSET
        if not isinstance(self.grouping, Unset):
            grouping = self.grouping.to_dict()

        pagination: Union[Unset, Dict[str, Any]] = UNSET
        if not isinstance(self.pagination, Unset):
            pagination = self.pagination.to_dict()

        query: Union[Unset, Dict[str, Any]] = UNSET
        if not isinstance(self.query, Unset):
            query = self.query.to_dict()

        query_name_aliases: Union[Unset, Dict[str, Any]] = UNSET
        if not isinstance(self.query_name_aliases, Unset):
            query_name_aliases = self.query_name_aliases.to_dict()

        sorting: Union[Unset, Dict[str, Any]] = UNSET
        if not isinstance(self.sorting, Unset):
            sorting = self.sorting.to_dict()

        truncate_string_to = self.truncate_string_to

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if attribute_filters is not UNSET:
            field_dict["attributeFilters"] = attribute_filters
        if experiment_leader is not UNSET:
            field_dict["experimentLeader"] = experiment_leader
        if grouping is not UNSET:
            field_dict["grouping"] = grouping
        if pagination is not UNSET:
            field_dict["pagination"] = pagination
        if query is not UNSET:
            field_dict["query"] = query
        if query_name_aliases is not UNSET:
            field_dict["queryNameAliases"] = query_name_aliases
        if sorting is not UNSET:
            field_dict["sorting"] = sorting
        if truncate_string_to is not UNSET:
            field_dict["truncateStringTo"] = truncate_string_to

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.nql_query_params_dto import NqlQueryParamsDTO
        from ..models.query_leaderboard_params_attribute_filter_dto import QueryLeaderboardParamsAttributeFilterDTO
        from ..models.query_leaderboard_params_grouping_params_dto import QueryLeaderboardParamsGroupingParamsDTO
        from ..models.query_leaderboard_params_pagination_dto import QueryLeaderboardParamsPaginationDTO
        from ..models.query_leaderboard_params_query_aliases_dto import QueryLeaderboardParamsQueryAliasesDTO
        from ..models.query_leaderboard_params_sorting_params_dto import QueryLeaderboardParamsSortingParamsDTO

        d = src_dict.copy()
        attribute_filters: Union[Unset, List[QueryLeaderboardParamsAttributeFilterDTO]] = UNSET
        _attribute_filters = d.pop("attributeFilters", UNSET)
        if not isinstance(_attribute_filters, Unset):
            attribute_filters = []
            for attribute_filters_item_data in _attribute_filters:
                attribute_filters_item = QueryLeaderboardParamsAttributeFilterDTO.from_dict(attribute_filters_item_data)

                attribute_filters.append(attribute_filters_item)

        experiment_leader = d.pop("experimentLeader", UNSET)

        _grouping = d.pop("grouping", UNSET)
        grouping: Union[Unset, QueryLeaderboardParamsGroupingParamsDTO]
        if isinstance(_grouping, Unset):
            grouping = UNSET
        else:
            grouping = QueryLeaderboardParamsGroupingParamsDTO.from_dict(_grouping)

        _pagination = d.pop("pagination", UNSET)
        pagination: Union[Unset, QueryLeaderboardParamsPaginationDTO]
        if isinstance(_pagination, Unset):
            pagination = UNSET
        else:
            pagination = QueryLeaderboardParamsPaginationDTO.from_dict(_pagination)

        _query = d.pop("query", UNSET)
        query: Union[Unset, NqlQueryParamsDTO]
        if isinstance(_query, Unset):
            query = UNSET
        else:
            query = NqlQueryParamsDTO.from_dict(_query)

        _query_name_aliases = d.pop("queryNameAliases", UNSET)
        query_name_aliases: Union[Unset, QueryLeaderboardParamsQueryAliasesDTO]
        if isinstance(_query_name_aliases, Unset):
            query_name_aliases = UNSET
        else:
            query_name_aliases = QueryLeaderboardParamsQueryAliasesDTO.from_dict(_query_name_aliases)

        _sorting = d.pop("sorting", UNSET)
        sorting: Union[Unset, QueryLeaderboardParamsSortingParamsDTO]
        if isinstance(_sorting, Unset):
            sorting = UNSET
        else:
            sorting = QueryLeaderboardParamsSortingParamsDTO.from_dict(_sorting)

        truncate_string_to = d.pop("truncateStringTo", UNSET)

        search_leaderboard_entries_params_dto = cls(
            attribute_filters=attribute_filters,
            experiment_leader=experiment_leader,
            grouping=grouping,
            pagination=pagination,
            query=query,
            query_name_aliases=query_name_aliases,
            sorting=sorting,
            truncate_string_to=truncate_string_to,
        )

        search_leaderboard_entries_params_dto.additional_properties = d
        return search_leaderboard_entries_params_dto

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
