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
)

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.api_error_type_dto import ApiErrorTypeDTO
from ..types import (
    UNSET,
    Unset,
)

T = TypeVar("T", bound="Error")


@_attrs_define
class Error:
    """
    Attributes:
        code (int):
        message (str):
        type (Union[Unset, ApiErrorTypeDTO]):
        error_type (Union[Unset, ApiErrorTypeDTO]):
    """

    code: int
    message: str
    type: Union[Unset, ApiErrorTypeDTO] = UNSET
    error_type: Union[Unset, ApiErrorTypeDTO] = UNSET
    additional_properties: Dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        code = self.code

        message = self.message

        type: Union[Unset, str] = UNSET
        if not isinstance(self.type, Unset):
            type = self.type.value

        error_type: Union[Unset, str] = UNSET
        if not isinstance(self.error_type, Unset):
            error_type = self.error_type.value

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "code": code,
                "message": message,
            }
        )
        if type is not UNSET:
            field_dict["type"] = type
        if error_type is not UNSET:
            field_dict["errorType"] = error_type

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        code = d.pop("code")

        message = d.pop("message")

        _type = d.pop("type", UNSET)
        type: Union[Unset, ApiErrorTypeDTO]
        if isinstance(_type, Unset):
            type = UNSET
        else:
            type = ApiErrorTypeDTO(_type)

        _error_type = d.pop("errorType", UNSET)
        error_type: Union[Unset, ApiErrorTypeDTO]
        if isinstance(_error_type, Unset):
            error_type = UNSET
        else:
            error_type = ApiErrorTypeDTO(_error_type)

        error = cls(
            code=code,
            message=message,
            type=type,
            error_type=error_type,
        )

        error.additional_properties = d
        return error

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
