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
    from ..models.time_series import TimeSeries


T = TypeVar("T", bound="FloatTimeSeriesValuesRequestSeries")


@_attrs_define
class FloatTimeSeriesValuesRequestSeries:
    """
    Attributes:
        request_id (str):
        series (TimeSeries):
        after_step (Union[Unset, float]):
    """

    request_id: str
    series: "TimeSeries"
    after_step: Union[Unset, float] = UNSET
    additional_properties: Dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        request_id = self.request_id

        series = self.series.to_dict()

        after_step = self.after_step

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "requestId": request_id,
                "series": series,
            }
        )
        if after_step is not UNSET:
            field_dict["afterStep"] = after_step

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.time_series import TimeSeries

        d = src_dict.copy()
        request_id = d.pop("requestId")

        series = TimeSeries.from_dict(d.pop("series"))

        after_step = d.pop("afterStep", UNSET)

        float_time_series_values_request_series = cls(
            request_id=request_id,
            series=series,
            after_step=after_step,
        )

        float_time_series_values_request_series.additional_properties = d
        return float_time_series_values_request_series

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
