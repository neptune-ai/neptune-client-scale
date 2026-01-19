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

T = TypeVar("T", bound="AttributeNameFilterDTO")


@_attrs_define
class AttributeNameFilterDTO:
    """
    Attributes:
        must_match_any (Union[Unset, List['AttributeNameFilterDTO']]): A list of filters representing disjunctions of
            conjunctive simple filters. The implementation is currently limited to 1 level of nesting. E.g. `(a AND b AND
            NOT c) OR (d AND e) OR (f AND g)` is supported, but `(a OR (b OR c))` is not supported. The latter should be
            expressed as `(a OR b OR c)`. `(a AND (b OR c))` is also not supported. Mutually exclusive with
            `mustMatchRegexes` and `mustNotMatchRegexes`.
        must_match_regexes (Union[Unset, List[str]]): An attribute must match all of the regexes from the list to be
            returned. Mutually exclusive with `mustMatchAny`
        must_not_match_regexes (Union[Unset, List[str]]): An attribute must match none of the regexes from the list to
            be returned. Mutually exclusive with `mustMatchAny`
    """

    must_match_any: Union[Unset, List["AttributeNameFilterDTO"]] = UNSET
    must_match_regexes: Union[Unset, List[str]] = UNSET
    must_not_match_regexes: Union[Unset, List[str]] = UNSET
    additional_properties: Dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        must_match_any: Union[Unset, List[Dict[str, Any]]] = UNSET
        if not isinstance(self.must_match_any, Unset):
            must_match_any = []
            for must_match_any_item_data in self.must_match_any:
                must_match_any_item = must_match_any_item_data.to_dict()
                must_match_any.append(must_match_any_item)

        must_match_regexes: Union[Unset, List[str]] = UNSET
        if not isinstance(self.must_match_regexes, Unset):
            must_match_regexes = self.must_match_regexes

        must_not_match_regexes: Union[Unset, List[str]] = UNSET
        if not isinstance(self.must_not_match_regexes, Unset):
            must_not_match_regexes = self.must_not_match_regexes

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if must_match_any is not UNSET:
            field_dict["mustMatchAny"] = must_match_any
        if must_match_regexes is not UNSET:
            field_dict["mustMatchRegexes"] = must_match_regexes
        if must_not_match_regexes is not UNSET:
            field_dict["mustNotMatchRegexes"] = must_not_match_regexes

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        must_match_any: Union[Unset, List[AttributeNameFilterDTO]] = UNSET
        _must_match_any = d.pop("mustMatchAny", UNSET)
        if not isinstance(_must_match_any, Unset):
            must_match_any = []
            for must_match_any_item_data in _must_match_any:
                must_match_any_item = AttributeNameFilterDTO.from_dict(must_match_any_item_data)

                must_match_any.append(must_match_any_item)

        must_match_regexes = cast(List[str], d.pop("mustMatchRegexes", UNSET))

        must_not_match_regexes = cast(List[str], d.pop("mustNotMatchRegexes", UNSET))

        attribute_name_filter_dto = cls(
            must_match_any=must_match_any,
            must_match_regexes=must_match_regexes,
            must_not_match_regexes=must_not_match_regexes,
        )

        attribute_name_filter_dto.additional_properties = d
        return attribute_name_filter_dto

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
