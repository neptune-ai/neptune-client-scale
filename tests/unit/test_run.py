import base64
import json
import uuid
from datetime import datetime

import pytest
from freezegun import freeze_time

from neptune_scale import Run


@pytest.fixture(scope="session")
def api_token():
    return base64.b64encode(json.dumps({"api_address": "aa", "api_url": "bb"}).encode("utf-8")).decode("utf-8")


# Set short timeouts on blocking operations for quicker test execution
@pytest.fixture(autouse=True, scope="session")
def short_timeouts():
    import neptune_scale
    import neptune_scale.core.components

    patch = pytest.MonkeyPatch()
    timeout = 0.05
    for name in (
        "MINIMAL_WAIT_FOR_PUT_SLEEP_TIME",
        "MINIMAL_WAIT_FOR_ACK_SLEEP_TIME",
        "STATUS_TRACKING_THREAD_SLEEP_TIME",
        "SYNC_THREAD_SLEEP_TIME",
        "ERRORS_MONITOR_THREAD_SLEEP_TIME",
    ):
        patch.setattr(neptune_scale.parameters, name, timeout)

        # Not perfect, but does the trick for now. Handle direct imports.
        for mod in (neptune_scale, neptune_scale.core.components.sync_process):
            if hasattr(mod, name):
                patch.setattr(mod, name, timeout)


def test_context_manager(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())
    family = run_id

    # when
    with Run(project=project, api_token=api_token, family=family, run_id=run_id, mode="disabled"):
        ...

    # then
    assert True


def test_close(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())
    family = run_id

    # and
    run = Run(project=project, api_token=api_token, family=family, run_id=run_id, mode="disabled")

    # when
    run.close()

    # then
    assert True


def test_family_too_long(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())

    # and
    family = "a" * 1000

    # when
    with pytest.raises(ValueError):
        with Run(project=project, api_token=api_token, family=family, run_id=run_id, mode="disabled"):
            ...

    # and
    assert True


def test_run_id_too_long(api_token):
    # given
    project = "workspace/project"
    family = str(uuid.uuid4())

    # and
    run_id = "a" * 1000

    # then
    with pytest.raises(ValueError):
        with Run(project=project, api_token=api_token, family=family, run_id=run_id, mode="disabled"):
            ...

    # and
    assert True


def test_invalid_project_name(api_token):
    # given
    run_id = str(uuid.uuid4())
    family = run_id

    # and
    project = "just-project"

    # then
    with pytest.raises(ValueError):
        with Run(project=project, api_token=api_token, family=family, run_id=run_id, mode="disabled"):
            ...

    # and
    assert True


def test_metadata(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())
    family = run_id

    # then
    with Run(project=project, api_token=api_token, family=family, run_id=run_id, mode="disabled") as run:
        run.log(
            step=1,
            timestamp=datetime.now(),
            fields={
                "int": 1,
                "string": "test",
                "float": 3.14,
                "bool": True,
                "datetime": datetime.now(),
            },
            metrics={
                "metric": 1.0,
            },
            add_tags={
                "tags": ["tag1"],
            },
            remove_tags={
                "group_tags": ["tag2"],
            },
        )

    # and
    assert True


def test_log_without_step(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())
    family = run_id

    # then
    with Run(project=project, api_token=api_token, family=family, run_id=run_id, mode="disabled") as run:
        run.log(
            timestamp=datetime.now(),
            fields={
                "int": 1,
            },
        )

    # and
    assert True


def test_log_step_float(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())
    family = run_id

    # then
    with Run(project=project, api_token=api_token, family=family, run_id=run_id, mode="disabled") as run:
        run.log(
            step=3.14,
            timestamp=datetime.now(),
            fields={
                "int": 1,
            },
        )

    # and
    assert True


def test_log_no_timestamp(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())
    family = run_id

    # then
    with Run(project=project, api_token=api_token, family=family, run_id=run_id, mode="disabled") as run:
        run.log(
            step=3.14,
            fields={
                "int": 1,
            },
        )

    # and
    assert True


def test_resume(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())
    family = run_id

    # when
    with Run(project=project, api_token=api_token, family=family, run_id=run_id, resume=True, mode="disabled"):
        ...

    # then
    assert True


@freeze_time("2024-07-30 12:12:12.000022")
def test_creation_time(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())
    family = run_id

    # when
    with Run(
        project=project,
        api_token=api_token,
        family=family,
        run_id=run_id,
        creation_time=datetime.now(),
        mode="disabled",
    ):
        ...

    # then
    assert True


def test_assign_experiment(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())
    family = run_id

    # when
    with Run(
        project=project,
        api_token=api_token,
        family=family,
        run_id=run_id,
        as_experiment="experiment_id",
        mode="disabled",
    ):
        ...

    # then
    assert True


def test_forking(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())
    family = run_id

    # when
    with Run(
        project=project,
        api_token=api_token,
        family=family,
        run_id=run_id,
        from_run_id="parent-run-id",
        from_step=3.14,
        mode="disabled",
    ):
        ...

    # then
    assert True
