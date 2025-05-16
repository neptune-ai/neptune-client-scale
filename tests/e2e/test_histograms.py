from typing import Union
from unittest.mock import patch

import numpy as np
import pytest

from neptune_scale.types import Histogram

from .conftest import unique_path
from .test_fetcher import fetch_series_values


@pytest.fixture(scope="module", autouse=True)
def raise_on_invalid_value():
    with patch("neptune_scale.sync.metadata_splitter.INVALID_VALUE_ACTION", "raise"):
        yield


@pytest.mark.skip(reason="Histograms are not public yet")
@pytest.mark.parametrize("values", ("counts", "densities"))
@pytest.mark.parametrize("use_numpy", (True, False), ids=("np.array", "list"))
def test_histograms(run, client, project_name, values, use_numpy):
    """Log multiple steps with multiple histograms, and verify that the values are correct. Do this
    once with counts and once with densities, and once with numpy arrays and once with plain lists."""

    path = unique_path("test_histograms")
    total_steps = 3

    histogram_lengths = [1, 2, 100, 512]
    step_to_logged_histograms = {
        step: {
            f"{path}/hist-{length}": Histogram(
                bin_edges=list(range(length + 1)),
                counts=_make_array(length, use_numpy) if values == "counts" else None,
                densities=_make_array(length, use_numpy) if values == "densities" else None,
            )
            for length in histogram_lengths
        }
        for step in range(total_steps)
    }

    for step, histograms in step_to_logged_histograms.items():
        run.log_histograms(histograms, step=step)

    run.wait_for_processing()

    attr_names = [f"{path}/hist-{length}" for length in histogram_lengths]
    fetched_histograms = fetch_series_values(client, project_name, attributes=attr_names, custom_run_id=run._run_id)

    # fetched histograms format is: path (str) -> dict of step (float) to histogram dict
    assert sorted(fetched_histograms.keys()) == sorted(attr_names), "Not all attributes are present"

    for path, series_values in fetched_histograms.items():
        assert len(series_values) == total_steps, f"Attribute {path} has incorrect number of steps"

        for step, histogram in series_values.items():
            expected = step_to_logged_histograms[int(step)][path]

            assert histogram["type"] == "COUNTING" if values == "counts" else "DENSITY"
            assert histogram["edges"] == expected.bin_edges
            expected_values = expected.counts if values == "counts" else expected.densities
            assert histogram["values"] == (expected_values.tolist() if use_numpy else expected_values)


def _make_array(length: int, use_numpy: bool) -> Union[list[int], np.ndarray]:
    return np.array(range(length)) if use_numpy else list(range(length))
