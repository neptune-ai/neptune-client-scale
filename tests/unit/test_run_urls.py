import uuid

import pytest

from neptune_scale import Run
from neptune_scale.util.envs import API_TOKEN_ENV_NAME


def test_run_url(api_token):
    run_id = str(uuid.uuid4())
    with Run(project="my-workspace-name/my-project-name", api_token=api_token, run_id=run_id, mode="offline") as run:
        run_url = run.run_url()

    assert "http://api-url" in run_url
    assert "my-workspace-name" in run_url
    assert "my-project-name" in run_url
    assert run_id in run_url


def test_experiment_url(api_token):
    with Run(
        project="my-workspace-name/my-project-name",
        experiment_name="my-experiment-name",
        api_token=api_token,
        run_id=str(uuid.uuid4()),
        mode="offline",
    ) as run:
        run_url = run.experiment_url()

    assert "http://api-url" in run_url
    assert "my-workspace-name" in run_url
    assert "my-project-name" in run_url
    assert "my-experiment-name" in run_url


def test_run_urls_no_api_token(monkeypatch, api_token):
    monkeypatch.delenv(API_TOKEN_ENV_NAME, raising=False)

    with Run(
        project="my-workspace-name/my-project-name",
        experiment_name="my-experiment-name",
        run_id=str(uuid.uuid4()),
        mode="offline",
    ) as run:
        with pytest.raises(ValueError) as exc:
            run.run_url()
        exc.match("API token was not provided")

        with pytest.raises(ValueError) as exc:
            run.experiment_url()
        exc.match("API token was not provided")


def test_experiment_url_no_experiment_name(api_token):
    with Run(
        project="my-workspace-name/my-project-name",
        api_token=api_token,
        run_id=str(uuid.uuid4()),
        mode="offline",
    ) as run:
        with pytest.raises(ValueError) as exc:
            run.experiment_url()
        exc.match("experiment_name.*was not provided")
