import importlib
import math
import sys
from datetime import datetime
from itertools import chain
from unittest.mock import patch

import numpy as np
import pytest
from freezegun import freeze_time
from google.protobuf.timestamp_pb2 import Timestamp

from neptune_scale.exceptions import NeptuneUnableToLogData
from neptune_scale.sync.metadata_splitter import histograms_to_update_run_snapshots
from neptune_scale.types import Histogram

# The character "𐍈" (U+10348) encodes to 4 bytes
UTF_CHAR = "𐍈"


def _arrays_equal_with_nan(a, b):
    """Compare two arrays for equality, treating NaN values as equal (which is not the case by default)."""
    return all((x == y) or (math.isnan(x) and math.isnan(y)) for x, y in zip(a, b))


def _as_list(arr) -> list:
    return arr.tolist() if isinstance(arr, np.ndarray) else arr


@pytest.fixture(params=(True, False), ids=("with_numpy", "without_numpy"))
def types_module(request):
    """
    When used in a test, this fixture returns the neptune_scale.types module with numpy
    enabled or disabled. Any testcase using this fixture will be executed 2x,
    once with numpy and once without it.
    """
    import neptune_scale.types

    orig_numpy = None

    if not request.param:
        orig_numpy = sys.modules.pop("numpy", None)
        sys.modules["numpy"] = None

    mod = importlib.reload(neptune_scale.types)

    assert mod._HAS_NUMPY == request.param, "Reimported module should have the same numpy status as requested"
    try:
        yield mod
    finally:
        # Always restore original modules for tests that don't use this fixture
        if orig_numpy:
            sys.modules["numpy"] = orig_numpy


@freeze_time("2024-07-30 12:12:12.000022")
@patch("neptune_scale.sync.metadata_splitter.INVALID_VALUE_ACTION", "raise")
@pytest.mark.parametrize(
    "bin_edges, counts, densities",
    (
        ([1, 2], [10], None),
        ([1, 2], None, [10]),
        (list(range(513)), list(range(512)), None),
        (list(range(513)), None, list(range(512))),
        ([-math.inf, 1, math.inf], [1, 2], None),
        ([-math.inf, 1, math.inf], None, [1, 2]),
        ([1, 2, 3], None, [1, math.nan]),
        ([1, 2, 3], None, [1, math.inf]),
        ([1, 2, 3], None, [1, -math.inf]),
    ),
)
@pytest.mark.parametrize("numpy_bin_edges", (False, True))
@pytest.mark.parametrize("numpy_counts", (False, True))
@pytest.mark.parametrize("numpy_densities", (False, True))
def test_histograms_to_operations(
    types_module, bin_edges, counts, densities, numpy_bin_edges, numpy_counts, numpy_densities
):
    if not types_module._HAS_NUMPY and any((numpy_bin_edges, numpy_counts, numpy_densities)):
        pytest.skip("Testcase with numpy disabled, skipping ndarray cases.")

    # If requested and not None, convert plain python lists to numpy arrays
    bin_edges = np.array(bin_edges) if numpy_bin_edges and bin_edges is not None else bin_edges
    counts = np.array(counts) if numpy_counts and counts is not None else counts
    densities = np.array(densities) if numpy_densities and densities is not None else densities

    # Note that we need a fairly large max_size for a single histogram to fit in
    max_size = 1024 * 10
    histograms = {
        f"histogram{i}": Histogram(bin_edges=bin_edges, counts=counts, densities=densities) for i in range(100)
    }

    timestamp = datetime.now()
    updates = histograms_to_update_run_snapshots(histograms, 1, timestamp=timestamp, max_size=max_size)

    result = list(updates)
    assert len(result)

    assert all(len(op.SerializeToString()) <= max_size for op in result)
    assert all(op.step.whole == 1 for op in result)
    assert all(op.timestamp == Timestamp(seconds=1722341532, nanos=21934) for op in result)

    assert all(not op.HasField("preview") for op in result), "preview should not be present"
    assert all(not op.assign for op in result), "no assigns should be set"
    assert all(not op.modify_sets for op in result), "no modify_sets should be set"

    # Validate generated proto Histogram values in all operations one by one
    for key, value in chain.from_iterable(op.append.items() for op in result):
        expected = histograms[key]
        assert value.histogram.bin_edges == _as_list(expected.bin_edges)

        if counts is not None:
            assert not value.histogram.HasField("densities")
            assert value.histogram.counts.values == _as_list(expected.counts)
        else:
            assert not value.histogram.HasField("counts")
            assert _arrays_equal_with_nan(value.histogram.densities.values, _as_list(expected.densities))


@pytest.mark.parametrize(
    "invalid_path",
    (
        None,
        "A" * 1025,
        "A" + UTF_CHAR * 256,
        UTF_CHAR * 257,
        object(),
        1,
        1.0,
        True,
        frozenset(),
        tuple(),
        datetime.now(),
    ),
)
@pytest.mark.parametrize("action", ("raise", "drop"))
def test_histograms_invalid_paths(caplog, action, invalid_path):
    data = {invalid_path: object()}
    with patch("neptune_scale.sync.metadata_splitter.INVALID_VALUE_ACTION", action):
        if action == "raise":
            with pytest.raises(NeptuneUnableToLogData, match="paths must be"):
                list(histograms_to_update_run_snapshots(data, 1, datetime.now()))
        else:
            with caplog.at_level("WARNING"):
                list(histograms_to_update_run_snapshots(data, 1, datetime.now()))
            assert "paths must be" in caplog.text


@pytest.mark.parametrize("path", ("A", "A" * 1024, UTF_CHAR * 256))
@patch("neptune_scale.sync.metadata_splitter.INVALID_VALUE_ACTION", "raise")
def test_histograms_valid_paths(path):
    data = {path: Histogram(bin_edges=[1], counts=[])}
    list(histograms_to_update_run_snapshots(data, 1, datetime.now()))


@pytest.mark.parametrize("action", ("raise", "drop"))
@pytest.mark.parametrize(
    "bin_edges, counts, densities, match_message",
    (
        (None, None, None, "Bin edges must be of type"),
        ([], None, None, "counts and densities must be set"),  # no values field is set
        ([], [], [], "cannot be set together"),
        ([1, 2], [1], [1], "cannot be set together"),  # counts and densities are both set
        ([1, 2], [1, 2], None, "counts must be of length equal to bin_edges - 1"),
        ([1, 2], list(range(100)), None, "counts must be of length equal to bin_edges - 1"),
        ([1, 2], None, [1, 2], "densities must be of length equal to bin_edges - 1"),
        ([1, 2], None, list(range(100)), "densities must be of length equal to bin_edges - 1"),
        (list(range(514)), list(range(513)), None, "bin_edges must be of length"),
        (list(range(1000)), list(range(999)), None, "bin_edges must be of length"),
        ([1, "s"], [1], None, "must be numeric"),
        ([1, 2, 3], [1, "s"], None, "must be numeric"),
        ([1, 2], [1.0], None, "must be numeric"),  # counts must be strictly ints
        ([1, 2], [math.nan], None, "must be numeric"),
        ([1, 2], [math.inf], None, "must be numeric"),
        ([1, 2], [-math.inf], None, "must be numeric"),
        ([1, 2, 3], None, [1, "s"], "must be numeric"),
    ),
)
@pytest.mark.parametrize("numpy_bin_edges", (False, True))
@pytest.mark.parametrize("numpy_counts", (False, True))
@pytest.mark.parametrize("numpy_densities", (False, True))
def test_histograms_invalid_values(
    caplog,
    types_module,
    action,
    bin_edges,
    counts,
    densities,
    match_message,
    numpy_bin_edges,
    numpy_counts,
    numpy_densities,
):
    if not types_module._HAS_NUMPY and any((numpy_bin_edges, numpy_counts, numpy_densities)):
        pytest.skip("Testcase with numpy disabled, skipping ndarray cases.")

    # If requested and not None, convert plain python lists to numpy arrays
    bin_edges = np.array(bin_edges) if numpy_bin_edges and bin_edges is not None else bin_edges
    counts = np.array(counts) if numpy_counts and counts is not None else counts
    densities = np.array(densities) if numpy_densities and densities is not None else densities

    data = {
        "bad-value": Histogram(bin_edges=bin_edges, counts=counts, densities=densities),
        "valid-value": Histogram(bin_edges=[1, 2], counts=[1]),
    }
    with patch("neptune_scale.sync.metadata_splitter.INVALID_VALUE_ACTION", action):
        if action == "raise":
            with pytest.raises(NeptuneUnableToLogData, match=match_message):
                list(histograms_to_update_run_snapshots(data, 1, datetime.now()))
        else:
            with caplog.at_level("WARNING"):
                result = list(histograms_to_update_run_snapshots(data, 1, datetime.now()))

            assert len(result[0].append) == 1
            assert "valid-value" in result[0].append
            assert match_message in caplog.text
            assert "Dropping value" in caplog.text
