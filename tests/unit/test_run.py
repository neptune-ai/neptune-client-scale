import base64
import json
import uuid
from datetime import datetime
from unittest.mock import patch

import pytest
from freezegun import freeze_time

from neptune_scale import Run


@pytest.fixture(scope="session")
def api_token():
    return base64.b64encode(json.dumps({"api_address": "aa", "api_url": "bb"}).encode("utf-8")).decode("utf-8")


class MockedApiClient:
    def __init__(self, *args, **kwargs) -> None:
        pass

    def submit(self, operation, family) -> None:
        pass

    def close(self) -> None:
        pass

    def cleanup(self) -> None:
        pass


@patch("neptune_scale.ApiClient", MockedApiClient)
def test_context_manager(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())
    family = run_id

    # when
    with Run(project=project, api_token=api_token, family=family, run_id=run_id):
        ...

    # then
    assert True


@patch("neptune_scale.ApiClient", MockedApiClient)
def test_close(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())
    family = run_id

    # and
    run = Run(project=project, api_token=api_token, family=family, run_id=run_id)

    # when
    run.close()

    # then
    assert True


@patch("neptune_scale.ApiClient", MockedApiClient)
def test_family_too_long(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())

    # and
    family = "a" * 1000

    # when
    with pytest.raises(ValueError):
        with Run(project=project, api_token=api_token, family=family, run_id=run_id):
            ...


@patch("neptune_scale.ApiClient", MockedApiClient)
def test_run_id_too_long(api_token):
    # given
    project = "workspace/project"
    family = str(uuid.uuid4())

    # and
    run_id = "a" * 1000

    # then
    with pytest.raises(ValueError):
        with Run(project=project, api_token=api_token, family=family, run_id=run_id):
            ...


@patch("neptune_scale.ApiClient", MockedApiClient)
def test_invalid_project_name(api_token):
    # given
    run_id = str(uuid.uuid4())
    family = run_id

    # and
    project = "just-project"

    # then
    with pytest.raises(ValueError):
        with Run(project=project, api_token=api_token, family=family, run_id=run_id):
            ...


@patch("neptune_scale.ApiClient", MockedApiClient)
def test_metadata(api_token):
    # given
    project = "workspace/project"
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


@patch("neptune_scale.ApiClient", MockedApiClient)
def test_log_without_step(api_token):
    # given
    project = "workspace/project"
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


@patch("neptune_scale.ApiClient", MockedApiClient)
def test_log_step_float(api_token):
    # given
    project = "workspace/project"
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


@patch("neptune_scale.ApiClient", MockedApiClient)
def test_log_no_timestamp(api_token):
    # given
    project = "workspace/project"
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


@patch("neptune_scale.ApiClient", MockedApiClient)
def test_resume(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())
    family = run_id

    # when
    with Run(project=project, api_token=api_token, family=family, run_id=run_id, resume=True):
        ...

    # then
    assert True


@patch("neptune_scale.ApiClient", MockedApiClient)
@freeze_time("2024-07-30 12:12:12.000022")
def test_creation_time(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())
    family = run_id

    # when
    with Run(project=project, api_token=api_token, family=family, run_id=run_id, creation_time=datetime.now()):
        ...

    # then
    assert True


@patch("neptune_scale.ApiClient", MockedApiClient)
def test_assign_experiment(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())
    family = run_id

    # when
    with Run(project=project, api_token=api_token, family=family, run_id=run_id, as_experiment="experiment_id"):
        ...

    # then
    assert True


@patch("neptune_scale.ApiClient", MockedApiClient)
def test_forking(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())
    family = run_id

    # when
    with Run(
        project=project, api_token=api_token, family=family, run_id=run_id, from_run_id="parent-run-id", from_step=3.14
    ):
        ...

    # then
    assert True
