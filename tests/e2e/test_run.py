import os
import time
from unittest.mock import patch

import pytest

from neptune_scale.api.run import Run

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")


@pytest.mark.parametrize("timeout", [0, 1, 5])
@pytest.mark.parametrize(
    "skipped_method",
    [
        "multiprocessing.context.SpawnProcess.terminate",
        "neptune_scale.sync.supervisor.ProcessSupervisor.interrupt",
        "neptune_scale.sync.errors_tracking.ErrorsMonitor.interrupt",
    ],
)
@pytest.mark.timeout(30)
def test_close_timeout(run_init_kwargs, timeout, skipped_method):
    # given
    run = Run(**run_init_kwargs)

    # when
    with patch(skipped_method):
        start_time = time.monotonic()
        run.close(timeout=timeout)
        elapsed_time = time.monotonic() - start_time

    # then
    assert timeout < elapsed_time < timeout + 1
