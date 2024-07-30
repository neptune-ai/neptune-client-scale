import uuid
from datetime import datetime

import pytest
from freezegun import freeze_time

from neptune_scale import Run


def test_context_manager():
    # given
    project = "workspace/project"
    api_token = "API_TOKEN"
    run_id = str(uuid.uuid4())
    family = run_id

    # when
    with Run(project=project, api_token=api_token, family=family, run_id=run_id):
        ...

    # then
    assert True


def test_close():
    # given
    project = "workspace/project"
    api_token = "API_TOKEN"
    run_id = str(uuid.uuid4())
    family = run_id

    # and
    run = Run(project=project, api_token=api_token, family=family, run_id=run_id)

    # when
    run.close()

    # then
    assert True


def test_family_too_long():
    # given
    project = "workspace/project"
    api_token = "API_TOKEN"
    run_id = str(uuid.uuid4())

    # and
    family = "a" * 1000

    # when
    with pytest.raises(ValueError):
        with Run(project=project, api_token=api_token, family=family, run_id=run_id):
            ...


def test_run_id_too_long():
    # given
    project = "workspace/project"
    api_token = "API_TOKEN"
    family = str(uuid.uuid4())

    # and
    run_id = "a" * 1000

    # then
    with pytest.raises(ValueError):
        with Run(project=project, api_token=api_token, family=family, run_id=run_id):
            ...


def test_invalid_project_name():
    # given
    api_token = "API_TOKEN"
    run_id = str(uuid.uuid4())
    family = run_id

    # and
    project = "just-project"

    # then
    with pytest.raises(ValueError):
        with Run(project=project, api_token=api_token, family=family, run_id=run_id):
            ...


def test_metadata():
    # given
    project = "workspace/project"
    api_token = "API_TOKEN"
    run_id = str(uuid.uuid4())
    family = run_id

    # then
    with Run(project=project, api_token=api_token, family=family, run_id=run_id) as run:
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


def test_log_without_step():
    # given
    project = "workspace/project"
    api_token = "API_TOKEN"
    run_id = str(uuid.uuid4())
    family = run_id

    # then
    with Run(project=project, api_token=api_token, family=family, run_id=run_id) as run:
        run.log(
            timestamp=datetime.now(),
            fields={
                "int": 1,
            },
        )


def test_log_step_float():
    # given
    project = "workspace/project"
    api_token = "API_TOKEN"
    run_id = str(uuid.uuid4())
    family = run_id

    # then
    with Run(project=project, api_token=api_token, family=family, run_id=run_id) as run:
        run.log(
            step=3.14,
            timestamp=datetime.now(),
            fields={
                "int": 1,
            },
        )


def test_log_no_timestamp():
    # given
    project = "workspace/project"
    api_token = "API_TOKEN"
    run_id = str(uuid.uuid4())
    family = run_id

    # then
    with Run(project=project, api_token=api_token, family=family, run_id=run_id) as run:
        run.log(
            step=3.14,
            fields={
                "int": 1,
            },
        )


def test_resume():
    # given
    project = "workspace/project"
    api_token = "API_TOKEN"
    run_id = str(uuid.uuid4())
    family = run_id

    # when
    with Run(project=project, api_token=api_token, family=family, run_id=run_id, resume=True):
        ...

    # then
    assert True


@freeze_time("2024-07-30 12:12:12.000022")
def test_creation_time():
    # given
    project = "workspace/project"
    api_token = "API_TOKEN"
    run_id = str(uuid.uuid4())
    family = run_id

    # when
    with Run(project=project, api_token=api_token, family=family, run_id=run_id, creation_time=datetime.now()):
        ...

    # then
    assert True


def test_assign_experiment():
    # given
    project = "workspace/project"
    api_token = "API_TOKEN"
    run_id = str(uuid.uuid4())
    family = run_id

    # when
    with Run(project=project, api_token=api_token, family=family, run_id=run_id, as_experiment="experiment_id"):
        ...

    # then
    assert True


def test_forking():
    # given
    project = "workspace/project"
    api_token = "API_TOKEN"
    run_id = str(uuid.uuid4())
    family = run_id

    # when
    with Run(
        project=project, api_token=api_token, family=family, run_id=run_id, from_run_id="parent-run-id", from_step=3.14
    ):
        ...

    # then
    assert True
