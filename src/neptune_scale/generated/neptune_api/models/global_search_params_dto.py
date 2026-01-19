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
    from ..models.query_leaderboard_params_pagination_dto import QueryLeaderboardParamsPaginationDTO
    from ..models.query_leaderboard_params_sorting_params_dto import QueryLeaderboardParamsSortingParamsDTO


T = TypeVar("T", bound="GlobalSearchParamsDTO")


@_attrs_define
class GlobalSearchParamsDTO:
    """
    Attributes:
        experiment_leader (Union[Unset, bool]):
        pagination (Union[Unset, QueryLeaderboardParamsPaginationDTO]):
        query (Union[Unset, NqlQueryParamsDTO]):
        sorting (Union[Unset, QueryLeaderboardParamsSortingParamsDTO]):
    """

    experiment_leader: Union[Unset, bool] = UNSET
    pagination: Union[Unset, "QueryLeaderboardParamsPaginationDTO"] = UNSET
    query: Union[Unset, "NqlQueryParamsDTO"] = UNSET
    sorting: Union[Unset, "QueryLeaderboardParamsSortingParamsDTO"] = UNSET
    additional_properties: Dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        experiment_leader = self.experiment_leader

        pagination: Union[Unset, Dict[str, Any]] = UNSET
        if not isinstance(self.pagination, Unset):
            pagination = self.pagination.to_dict()

        query: Union[Unset, Dict[str, Any]] = UNSET
        if not isinstance(self.query, Unset):
            query = self.query.to_dict()

        sorting: Union[Unset, Dict[str, Any]] = UNSET
        if not isinstance(self.sorting, Unset):
            sorting = self.sorting.to_dict()

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if experiment_leader is not UNSET:
            field_dict["experimentLeader"] = experiment_leader
        if pagination is not UNSET:
            field_dict["pagination"] = pagination
        if query is not UNSET:
            field_dict["query"] = query
        if sorting is not UNSET:
            field_dict["sorting"] = sorting

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.nql_query_params_dto import NqlQueryParamsDTO
        from ..models.query_leaderboard_params_pagination_dto import QueryLeaderboardParamsPaginationDTO
        from ..models.query_leaderboard_params_sorting_params_dto import QueryLeaderboardParamsSortingParamsDTO

        d = src_dict.copy()
        experiment_leader = d.pop("experimentLeader", UNSET)

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

        _sorting = d.pop("sorting", UNSET)
        sorting: Union[Unset, QueryLeaderboardParamsSortingParamsDTO]
        if isinstance(_sorting, Unset):
            sorting = UNSET
        else:
            sorting = QueryLeaderboardParamsSortingParamsDTO.from_dict(_sorting)

        global_search_params_dto = cls(
            experiment_leader=experiment_leader,
            pagination=pagination,
            query=query,
            sorting=sorting,
        )

        global_search_params_dto.additional_properties = d
        return global_search_params_dto

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
