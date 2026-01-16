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
    from ..models.attribute_filter_dto import AttributeFilterDTO
    from ..models.attribute_name_filter_dto import AttributeNameFilterDTO
    from ..models.next_page_dto import NextPageDTO


T = TypeVar("T", bound="QueryAttributesBodyDTO")


@_attrs_define
class QueryAttributesBodyDTO:
    """
    Attributes:
        attribute_filter (Union[Unset, List['AttributeFilterDTO']]): Filter by attribute (match any), if null no type or
            property value filtering is applied
        attribute_name_filter (Union[Unset, AttributeNameFilterDTO]):
        attribute_names_filter (Union[Unset, List[str]]): Filter by attribute name, if null all attributes are
            considered
        experiment_ids_filter (Union[Unset, List[str]]): Filter by experiment id, if null all experiments are considered
        next_page (Union[Unset, NextPageDTO]):
    """

    attribute_filter: Union[Unset, List["AttributeFilterDTO"]] = UNSET
    attribute_name_filter: Union[Unset, "AttributeNameFilterDTO"] = UNSET
    attribute_names_filter: Union[Unset, List[str]] = UNSET
    experiment_ids_filter: Union[Unset, List[str]] = UNSET
    next_page: Union[Unset, "NextPageDTO"] = UNSET
    additional_properties: Dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        attribute_filter: Union[Unset, List[Dict[str, Any]]] = UNSET
        if not isinstance(self.attribute_filter, Unset):
            attribute_filter = []
            for attribute_filter_item_data in self.attribute_filter:
                attribute_filter_item = attribute_filter_item_data.to_dict()
                attribute_filter.append(attribute_filter_item)

        attribute_name_filter: Union[Unset, Dict[str, Any]] = UNSET
        if not isinstance(self.attribute_name_filter, Unset):
            attribute_name_filter = self.attribute_name_filter.to_dict()

        attribute_names_filter: Union[Unset, List[str]] = UNSET
        if not isinstance(self.attribute_names_filter, Unset):
            attribute_names_filter = self.attribute_names_filter

        experiment_ids_filter: Union[Unset, List[str]] = UNSET
        if not isinstance(self.experiment_ids_filter, Unset):
            experiment_ids_filter = self.experiment_ids_filter

        next_page: Union[Unset, Dict[str, Any]] = UNSET
        if not isinstance(self.next_page, Unset):
            next_page = self.next_page.to_dict()

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if attribute_filter is not UNSET:
            field_dict["attributeFilter"] = attribute_filter
        if attribute_name_filter is not UNSET:
            field_dict["attributeNameFilter"] = attribute_name_filter
        if attribute_names_filter is not UNSET:
            field_dict["attributeNamesFilter"] = attribute_names_filter
        if experiment_ids_filter is not UNSET:
            field_dict["experimentIdsFilter"] = experiment_ids_filter
        if next_page is not UNSET:
            field_dict["nextPage"] = next_page

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.attribute_filter_dto import AttributeFilterDTO
        from ..models.attribute_name_filter_dto import AttributeNameFilterDTO
        from ..models.next_page_dto import NextPageDTO

        d = src_dict.copy()
        attribute_filter: Union[Unset, List[AttributeFilterDTO]] = UNSET
        _attribute_filter = d.pop("attributeFilter", UNSET)
        if not isinstance(_attribute_filter, Unset):
            attribute_filter = []
            for attribute_filter_item_data in _attribute_filter:
                attribute_filter_item = AttributeFilterDTO.from_dict(attribute_filter_item_data)

                attribute_filter.append(attribute_filter_item)

        _attribute_name_filter = d.pop("attributeNameFilter", UNSET)
        attribute_name_filter: Union[Unset, AttributeNameFilterDTO]
        if isinstance(_attribute_name_filter, Unset):
            attribute_name_filter = UNSET
        else:
            attribute_name_filter = AttributeNameFilterDTO.from_dict(_attribute_name_filter)

        attribute_names_filter = cast(List[str], d.pop("attributeNamesFilter", UNSET))

        experiment_ids_filter = cast(List[str], d.pop("experimentIdsFilter", UNSET))

        _next_page = d.pop("nextPage", UNSET)
        next_page: Union[Unset, NextPageDTO]
        if isinstance(_next_page, Unset):
            next_page = UNSET
        else:
            next_page = NextPageDTO.from_dict(_next_page)

        query_attributes_body_dto = cls(
            attribute_filter=attribute_filter,
            attribute_name_filter=attribute_name_filter,
            attribute_names_filter=attribute_names_filter,
            experiment_ids_filter=experiment_ids_filter,
            next_page=next_page,
        )

        query_attributes_body_dto.additional_properties = d
        return query_attributes_body_dto

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
