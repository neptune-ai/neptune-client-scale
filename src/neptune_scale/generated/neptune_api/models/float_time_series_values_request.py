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

from ..models.float_time_series_values_request_order import FloatTimeSeriesValuesRequestOrder
from ..types import (
    UNSET,
    Unset,
)

if TYPE_CHECKING:
    from ..models.float_time_series_values_request_series import FloatTimeSeriesValuesRequestSeries
    from ..models.open_range_dto import OpenRangeDTO


T = TypeVar("T", bound="FloatTimeSeriesValuesRequest")


@_attrs_define
class FloatTimeSeriesValuesRequest:
    """
    Attributes:
        per_series_points_limit (int):
        requests (List['FloatTimeSeriesValuesRequestSeries']):
        order (Union[Unset, FloatTimeSeriesValuesRequestOrder]):
        step_range (Union[Unset, OpenRangeDTO]):
    """

    per_series_points_limit: int
    requests: List["FloatTimeSeriesValuesRequestSeries"]
    order: Union[Unset, FloatTimeSeriesValuesRequestOrder] = UNSET
    step_range: Union[Unset, "OpenRangeDTO"] = UNSET
    additional_properties: Dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        per_series_points_limit = self.per_series_points_limit

        requests = []
        for requests_item_data in self.requests:
            requests_item = requests_item_data.to_dict()
            requests.append(requests_item)

        order: Union[Unset, str] = UNSET
        if not isinstance(self.order, Unset):
            order = self.order.value

        step_range: Union[Unset, Dict[str, Any]] = UNSET
        if not isinstance(self.step_range, Unset):
            step_range = self.step_range.to_dict()

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "perSeriesPointsLimit": per_series_points_limit,
                "requests": requests,
            }
        )
        if order is not UNSET:
            field_dict["order"] = order
        if step_range is not UNSET:
            field_dict["stepRange"] = step_range

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.float_time_series_values_request_series import FloatTimeSeriesValuesRequestSeries
        from ..models.open_range_dto import OpenRangeDTO

        d = src_dict.copy()
        per_series_points_limit = d.pop("perSeriesPointsLimit")

        requests = []
        _requests = d.pop("requests")
        for requests_item_data in _requests:
            requests_item = FloatTimeSeriesValuesRequestSeries.from_dict(requests_item_data)

            requests.append(requests_item)

        _order = d.pop("order", UNSET)
        order: Union[Unset, FloatTimeSeriesValuesRequestOrder]
        if isinstance(_order, Unset):
            order = UNSET
        else:
            order = FloatTimeSeriesValuesRequestOrder(_order)

        _step_range = d.pop("stepRange", UNSET)
        step_range: Union[Unset, OpenRangeDTO]
        if isinstance(_step_range, Unset):
            step_range = UNSET
        else:
            step_range = OpenRangeDTO.from_dict(_step_range)

        float_time_series_values_request = cls(
            per_series_points_limit=per_series_points_limit,
            requests=requests,
            order=order,
            step_range=step_range,
        )

        float_time_series_values_request.additional_properties = d
        return float_time_series_values_request

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
