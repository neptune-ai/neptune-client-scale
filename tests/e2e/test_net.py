import os

from neptune_scale.net.runs import run_exists

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")


def test_run_exists_true(run):
    assert run_exists(run._project, run._run_id)
    assert not run_exists(run._project, "nonexistent_run_id")
