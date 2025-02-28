from datetime import datetime

import pytest

from neptune_scale.api.series_step import SeriesStep
from neptune_scale.sync.parameters import MAX_STRING_SERIES_DATA_POINT_LENGTH


def test_series_step_valid_types():
    SeriesStep(data={"int": 1, "str": "hello", "float": 1.2}, step=1, preview=True, preview_completion=0.1)


def test_series_step_invalid_data_type():
    with pytest.raises(TypeError) as exc:
        SeriesStep(data=["invalid", "data"], step=1)
    exc.match("data must be a")


@pytest.mark.parametrize("value", [object(), datetime.now(), set(), list(), dict()])
def test_series_step_invalid_data_point_type(value):
    with pytest.raises(TypeError) as exc:
        SeriesStep(data={"metric1": value}, step=1)
    exc.match(r"data\['metric1'\] must be a")


def test_series_step_too_long_string_data_point():
    with pytest.raises(ValueError) as exc:
        SeriesStep(data={"metric1": "a" * (MAX_STRING_SERIES_DATA_POINT_LENGTH + 1)}, step=1)
    exc.match(r"data\['metric1'\].*must not exceed")


def test_series_step_invalid_step_type():
    with pytest.raises(TypeError) as exc:
        SeriesStep(data={"metric1": 1.0}, step="invalid")
    exc.match("step must be a")


def test_series_step_invalid_preview_type():
    with pytest.raises(TypeError) as exc:
        SeriesStep(data={"metric1": 1.0}, step=1, preview="invalid")
    exc.match("preview must be a")


def test_series_step_invalid_preview_completion_type():
    with pytest.raises(TypeError) as exc:
        SeriesStep(data={"metric1": 1.0}, step=1, preview_completion="invalid")
    exc.match("preview_completion must be a")


def test_series_step_invalid_preview_completion_value():
    with pytest.raises(ValueError) as exc:
        SeriesStep(data={"metric1": 1.0}, step=1, preview=True, preview_completion=1.5)
    exc.match("preview_completion must have a value between")

    with pytest.raises(ValueError) as exc:
        SeriesStep(data={"metric1": 1.0}, step=1, preview=True, preview_completion=-1.0)
    exc.match("preview_completion must have a value between")


def test_preview_completion_is_none_when_preview_is_false():
    step = SeriesStep(data={"metric1": 1.0}, step=1, preview=False)
    assert step.preview_completion is None


def test_preview_completion_only_when_preview_is_true():
    with pytest.raises(ValueError) as exc:
        SeriesStep(data={"metric1": 1.0}, step=1, preview=False, preview_completion=0.1)
    exc.match("preview_completion can only be specified")
