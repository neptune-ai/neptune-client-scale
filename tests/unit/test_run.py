import uuid
from datetime import datetime

import pytest
from freezegun import freeze_time

from neptune_scale import Run


# Set short timeouts on blocking operations for quicker test execution
@pytest.fixture(autouse=True, scope="session")
def short_timeouts():
    import neptune_scale.sync.parameters
    import neptune_scale.sync.sync_process

    patch = pytest.MonkeyPatch()
    timeout = 0.05
    for name in (
        "MINIMAL_WAIT_FOR_PUT_SLEEP_TIME",
        "MINIMAL_WAIT_FOR_ACK_SLEEP_TIME",
        "STATUS_TRACKING_THREAD_SLEEP_TIME",
        "SYNC_THREAD_SLEEP_TIME",
        "ERRORS_MONITOR_THREAD_SLEEP_TIME",
    ):
        patch.setattr(neptune_scale.sync.parameters, name, timeout)

        # Not perfect, but does the trick for now. Handle direct imports.
        for mod in (neptune_scale, neptune_scale.sync.sync_process):
            if hasattr(mod, name):
                patch.setattr(mod, name, timeout)


def test_context_manager(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())

    # when
    with Run(project=project, api_token=api_token, run_id=run_id, mode="disabled"):
        ...

    # then
    assert True


def test_close(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())

    # and
    run = Run(project=project, api_token=api_token, run_id=run_id, mode="disabled")

    # when
    run.close()

    # then
    assert True


def test_run_id_too_long(api_token):
    # given
    project = "workspace/project"

    # and
    run_id = "a" * 1000

    # then
    with pytest.raises(ValueError):
        with Run(project=project, api_token=api_token, run_id=run_id, mode="disabled"):
            ...

    # and
    assert True


def test_invalid_project_name(api_token):
    # given
    run_id = str(uuid.uuid4())

    # and
    project = "just-project"

    # then
    with pytest.raises(ValueError):
        with Run(project=project, api_token=api_token, run_id=run_id, mode="disabled"):
            ...

    # and
    assert True


def test_metadata(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())

    # then
    with Run(project=project, api_token=api_token, run_id=run_id, mode="disabled") as run:
        run.log(
            step=1,
            timestamp=datetime.now(),
            configs={
                "int": 1,
                "string": "test",
                "float": 3.14,
                "bool": True,
                "datetime": datetime.now(),
            },
            metrics={
                "metric": 3.14,
                "metric2": 5,
            },
            tags_add={
                "tags": ["tag1"],
            },
            tags_remove={
                "group_tags": ["tag2"],
            },
        )

    # and
    assert True


def test_tags(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())

    # then
    with Run(project=project, api_token=api_token, run_id=run_id, mode="disabled") as run:
        run.add_tags(["tag1", "tag2"])
        run.add_tags(["tag3", "tag4"], group_tags=True)
        run.remove_tags(["tag1"])
        run.remove_tags(["tag3"], group_tags=True)
        run.add_tags(("tag5", "tag6"))
        run.remove_tags(("tag5", "tag6"))
        run.add_tags(("tag5", "tag6"), group_tags=True)
        run.remove_tags(("tag5", "tag6"), group_tags=True)
        run.add_tags({"tag7", "tag8"})
        run.remove_tags({"tag7", "tag8"})
        run.add_tags({"tag7", "tag8"}, group_tags=True)
        run.remove_tags({"tag7", "tag8"}, group_tags=True)

    # and
    assert True


def test_log_without_step(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())

    # then
    with Run(project=project, api_token=api_token, run_id=run_id, mode="disabled") as run:
        run.log(
            timestamp=datetime.now(),
            configs={
                "int": 1,
            },
        )

    # and
    assert True


def test_log_configs(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())

    # then
    with Run(project=project, api_token=api_token, run_id=run_id, mode="disabled") as run:
        run.log_configs({"int": 1})
        run.log_configs({"float": 3.14})
        run.log_configs({"bool": True})
        run.log_configs({"string": "test"})
        run.log_configs({"datetime": datetime.now()})
        run.log_configs({"string_list": ["a", "b", "c"]})

    # and
    assert True


def test_log_step_float(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())

    # then
    with Run(project=project, api_token=api_token, run_id=run_id, mode="disabled") as run:
        run.log(
            step=3.14,
            timestamp=datetime.now(),
            configs={
                "int": 1,
            },
        )

    # and
    assert True


def test_log_no_timestamp(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())

    # then
    with Run(project=project, api_token=api_token, run_id=run_id, mode="disabled") as run:
        run.log(
            step=3.14,
            configs={
                "int": 1,
            },
        )

    # and
    assert True


def test_resume(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())

    # when
    with Run(project=project, api_token=api_token, run_id=run_id, resume=True, mode="disabled") as run:
        run.log(
            step=3.14,
            configs={
                "int": 1,
            },
        )

    # then
    assert True


@freeze_time("2024-07-30 12:12:12.000022")
def test_creation_time(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())

    # when
    with Run(
        project=project,
        api_token=api_token,
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

    # when
    with Run(
        project=project,
        api_token=api_token,
        run_id=run_id,
        experiment_name="experiment_id",
        mode="disabled",
    ):
        ...

    # then
    assert True


def test_forking(api_token):
    # given
    project = "workspace/project"
    run_id = str(uuid.uuid4())

    # when
    with Run(
        project=project,
        api_token=api_token,
        run_id=run_id,
        fork_run_id="parent-run-id",
        fork_step=3.14,
        mode="disabled",
    ):
        ...

    # then
    assert True


def test_too_large_string_series_datapoint_raises_error(api_token):
    project = "workspace/project"
    run_id = str(uuid.uuid4())

    with Run(
        project=project,
        api_token=api_token,
        run_id=run_id,
        fork_run_id="parent-run-id",
        fork_step=3.14,
        mode="disabled",
    ) as run:
        with pytest.raises(ValueError) as exc:
            run.log_string_series(data={"attr-name": "a" * 2 * 1024**2}, step=1)

        exc.match(r"data\['attr-name'\] must not exceed.*bytes")
