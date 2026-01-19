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

from ..models.provider import Provider
from ..types import (
    UNSET,
    Unset,
)

if TYPE_CHECKING:
    from ..models.multipart_upload import MultipartUpload


T = TypeVar("T", bound="SignedFile")


@_attrs_define
class SignedFile:
    """
    Attributes:
        path (str):
        project_identifier (str):
        provider (Provider):
        url (str):
        multipart (Union['MultipartUpload', None, Unset]):
    """

    path: str
    project_identifier: str
    provider: Provider
    url: str
    multipart: Union["MultipartUpload", None, Unset] = UNSET
    additional_properties: Dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        from ..models.multipart_upload import MultipartUpload

        path = self.path

        project_identifier = self.project_identifier

        provider = self.provider.value

        url = self.url

        multipart: Union[Dict[str, Any], None, Unset]
        if isinstance(self.multipart, Unset):
            multipart = UNSET
        elif isinstance(self.multipart, MultipartUpload):
            multipart = self.multipart.to_dict()
        else:
            multipart = self.multipart

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "path": path,
                "project_identifier": project_identifier,
                "provider": provider,
                "url": url,
            }
        )
        if multipart is not UNSET:
            field_dict["multipart"] = multipart

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.multipart_upload import MultipartUpload

        d = src_dict.copy()
        path = d.pop("path")

        project_identifier = d.pop("project_identifier")

        provider = Provider(d.pop("provider"))

        url = d.pop("url")

        def _parse_multipart(data: object) -> Union["MultipartUpload", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                multipart_type_1 = MultipartUpload.from_dict(data)

                return multipart_type_1
            except:  # noqa: E722
                pass
            return cast(Union["MultipartUpload", None, Unset], data)

        multipart = _parse_multipart(d.pop("multipart", UNSET))

        signed_file = cls(
            path=path,
            project_identifier=project_identifier,
            provider=provider,
            url=url,
            multipart=multipart,
        )

        signed_file.additional_properties = d
        return signed_file

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
