import uuid

import pytest

from neptune_scale import Run
from neptune_scale.exceptions import NeptuneApiTokenNotProvided
from neptune_scale.util.envs import API_TOKEN_ENV_NAME


def test_run_url(api_token):
    run_id = str(uuid.uuid4())
    with Run(
        project="workspace-name-漢/project-name-✓", api_token=api_token, run_id=run_id + "&?", mode="offline"
    ) as run:
        run_url = run.get_run_url()

    assert "http://api-url" in run_url
    assert "workspace-name-%E6%BC%A2" in run_url
    assert "project-name-%E2%9C%93" in run_url
    assert f"{run_id}%26%3F" in run_url


def test_experiment_url(api_token):
    with Run(
        project="workspace-name-漢/project-name-✓",
        experiment_name="my-experiment-name&?",
        api_token=api_token,
        run_id=str(uuid.uuid4()),
        mode="offline",
    ) as run:
        run_url = run.get_experiment_url()

    assert "http://api-url" in run_url
    assert "workspace-name-%E6%BC%A2" in run_url
    assert "project-name-%E2%9C%93" in run_url
    assert "experiment-name%26%3F" in run_url


def test_run_urls_no_api_token(monkeypatch, api_token):
    monkeypatch.delenv(API_TOKEN_ENV_NAME, raising=False)

    with Run(
        project="my-workspace-name/my-project-name",
        experiment_name="my-experiment-name",
        run_id=str(uuid.uuid4()),
        mode="offline",
    ) as run:
        with pytest.raises(NeptuneApiTokenNotProvided) as exc:
            run.get_run_url()
        exc.match("API token was not provided")

        with pytest.raises(NeptuneApiTokenNotProvided) as exc:
            run.get_experiment_url()
        exc.match("API token was not provided")


def test_experiment_url_no_experiment_name(api_token):
    with Run(
        project="my-workspace-name/my-project-name",
        api_token=api_token,
        run_id=str(uuid.uuid4()),
        mode="offline",
    ) as run:
        with pytest.raises(ValueError) as exc:
            run.get_experiment_url()
        exc.match("experiment_name.*was not provided")
