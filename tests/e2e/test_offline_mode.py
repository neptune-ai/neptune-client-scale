import os
import tempfile
import uuid

from neptune_fetcher import ReadOnlyRun
from pytest import fixture
from util import random_series

from neptune_scale import Run
from neptune_scale.cli.sync import sync_file
from neptune_scale.storage.operations import database_path_for_run

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")


@fixture(autouse=True)
def log_dir(monkeypatch):
    """Run all tests with a temp directory used for logging offline data"""

    with tempfile.TemporaryDirectory() as tmp:
        monkeypatch.setenv("NEPTUNE_LOG_DIR", tmp)
        yield tmp


@fixture
def run_id(run_init_kwargs):
    return str(uuid.uuid4())


@fixture
def project_name():
    return NEPTUNE_PROJECT


@fixture
def run(project_name, run_id):
    return Run(project=project_name, run_id=run_id, mode="offline")


def neptune_sync(project_name, run_id, temporary_log_dir):
    path = database_path_for_run(project_name, run_id, temporary_log_dir)
    sync_file(
        path,
        api_token=None,
        allow_non_increasing_step=False,
        parent_must_exist=True,
    )


def test_sync_fresh_run(project, project_name, run_id, run, log_dir):
    steps, values = random_series(length=5)

    with run:
        for step, value in zip(steps, values):
            run.log_metrics(data={"series": value}, step=step)

        run.log_configs({"foo": "bar"})

    neptune_sync(project_name, run_id, log_dir)

    ro_run = ReadOnlyRun(project, custom_id=run_id)
    assert ro_run["foo"].fetch() == "bar"

    df = ro_run["series"].fetch_values()
    assert df["step"].tolist() == steps
    assert df["value"].tolist() == values


def test_sync_resumed_run(project, project_name, run_id, run, log_dir):
    steps, values = random_series(length=5)

    # Log part of the data
    with run:
        for step, value in zip(steps[:2], values[:2]):
            run.log_metrics(data={"series": value}, step=step)

    # Resume run and log the remaining data
    run = Run(project=project_name, run_id=run_id, mode="offline", resume=True)
    with run:
        for step, value in zip(steps[2:], values[2:]):
            run.log_metrics(data={"series": value}, step=step)
        run.log_configs({"foo": "bar"})

    neptune_sync(project_name, run_id, log_dir)

    ro_run = ReadOnlyRun(project, custom_id=run_id)
    assert ro_run["foo"].fetch() == "bar"

    df = ro_run["series"].fetch_values()
    assert df["step"].tolist() == steps
    assert df["value"].tolist() == values
