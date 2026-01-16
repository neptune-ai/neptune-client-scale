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

from ..models.query_leaderboard_params_sorting_params_dto_dir import QueryLeaderboardParamsSortingParamsDTODir
from ..types import (
    UNSET,
    Unset,
)

if TYPE_CHECKING:
    from ..models.query_leaderboard_params_field_dto import QueryLeaderboardParamsFieldDTO


T = TypeVar("T", bound="QueryLeaderboardParamsSortingParamsDTO")


@_attrs_define
class QueryLeaderboardParamsSortingParamsDTO:
    """
    Attributes:
        sort_by (QueryLeaderboardParamsFieldDTO):
        dir_ (Union[Unset, QueryLeaderboardParamsSortingParamsDTODir]):
    """

    sort_by: "QueryLeaderboardParamsFieldDTO"
    dir_: Union[Unset, QueryLeaderboardParamsSortingParamsDTODir] = UNSET
    additional_properties: Dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        sort_by = self.sort_by.to_dict()

        dir_: Union[Unset, str] = UNSET
        if not isinstance(self.dir_, Unset):
            dir_ = self.dir_.value

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "sortBy": sort_by,
            }
        )
        if dir_ is not UNSET:
            field_dict["dir"] = dir_

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.query_leaderboard_params_field_dto import QueryLeaderboardParamsFieldDTO

        d = src_dict.copy()
        sort_by = QueryLeaderboardParamsFieldDTO.from_dict(d.pop("sortBy"))

        _dir_ = d.pop("dir", UNSET)
        dir_: Union[Unset, QueryLeaderboardParamsSortingParamsDTODir]
        if isinstance(_dir_, Unset):
            dir_ = UNSET
        else:
            dir_ = QueryLeaderboardParamsSortingParamsDTODir(_dir_)

        query_leaderboard_params_sorting_params_dto = cls(
            sort_by=sort_by,
            dir_=dir_,
        )

        query_leaderboard_params_sorting_params_dto.additional_properties = d
        return query_leaderboard_params_sorting_params_dto

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
