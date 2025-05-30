import logging
import os
import queue
import random
import sys
import tempfile
import time
import uuid
from datetime import (
    datetime,
    timezone,
)
from pathlib import Path

from neptune_api import AuthenticatedClient
from pytest import fixture

from neptune_scale import Run
from neptune_scale.util.envs import PROJECT_ENV_NAME

from .test_fetcher.client import create_client


@fixture(scope="session", autouse=True)
def cleanup_logging_handlers():
    """Remove all logging handlers after each test session, to avoid
    messy errors from the `logging` module.

    The errors happen because `Run` installs `Run.close` as an `atexit` handler.
    The method logs some messages. The output is captured by pytest, which closes
    its logging handler early, causing "ValueError: I/O operation on closed file."
    """

    try:
        yield
    finally:
        logger = logging.getLogger("neptune")
        logger.handlers.clear()


@fixture(scope="module")
def run_init_kwargs(project_name):
    """Arguments to initialize a neptune_scale.Run instance"""

    # TODO: if a test fails the run could be left in an indefinite state
    #       Maybe we should just have it scoped 'function' and require passing
    #       an existing run id
    kwargs = {"project": str(project_name)}
    run_id = os.getenv("NEPTUNE_E2E_CUSTOM_RUN_ID")
    if not run_id:
        run_id = str(uuid.uuid4())
        kwargs["experiment_name"] = "pye2e-scale"
    else:
        kwargs["resume"] = True

    kwargs["run_id"] = run_id

    return kwargs


@fixture(scope="module")
def on_error_queue():
    return queue.Queue()


@fixture(scope="module")
def run(run_init_kwargs, on_error_queue):
    """Plain neptune_scale.Run instance. We're scoping it to "module", as it seems to be a
    good compromise, mostly because of execution time."""

    def error_callback(error, last_seen_at):
        on_error_queue.put(error)

    run = Run(on_error_callback=error_callback, **run_init_kwargs)
    run.log_configs({"test_start_time": datetime.now(timezone.utc)})
    run.wait_for_processing()

    yield run

    run.terminate()


@fixture
def temp_dir():
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir).resolve()
    except Exception:
        # There are issues with windows workers: the temporary dir is being
        # held busy which results in an error during cleanup. We ignore these for now.
        if sys.platform != "win32":
            raise


def unique_path(prefix):
    return f"{prefix}__{datetime.now(timezone.utc).isoformat('-', 'seconds')}__{str(uuid.uuid4())[-4:]}"


def random_series(length=10, start_step=0):
    """Return a 2-tuple of step and value lists, both of length `length`"""
    assert length > 0
    assert start_step >= 0

    j = random.random()
    # Round to 0 to avoid floating point errors
    steps = [round((j + x) ** 2.0, 0) for x in range(start_step, length)]
    values = [round((j + x) ** 3.0, 0) for x in range(len(steps))]

    return steps, values


@fixture(scope="module")
def project_name(request) -> str:
    # Assume the project name and API token are set in the environment using the standard
    # NEPTUNE_PROJECT and NEPTUNE_API_TOKEN variables.
    #
    # We also allow overriding the project name per module by setting the
    # module-level `NEPTUNE_PROJECT` variable.

    project_name = getattr(request.module, "NEPTUNE_PROJECT", None)
    if project_name is None:
        project_name = os.getenv(PROJECT_ENV_NAME)
    return project_name


@fixture(scope="session")
def client() -> AuthenticatedClient:
    return create_client()


def sleep_3s(**kwargs):
    time.sleep(3)


def sleep_10s(**kwargs):
    time.sleep(10)
