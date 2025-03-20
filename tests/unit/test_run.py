import sys
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from freezegun import freeze_time

from neptune_scale import Run
from neptune_scale.sync.operations_repository import OperationsRepository
from neptune_scale.util import envs


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


@pytest.mark.parametrize("mode", ["disabled", "offline"])
def test_context_manager(api_token, mode):
    # given
    project = "workspace/project"

    # when
    with Run(project=project, api_token=api_token, mode=mode):
        ...

    # then
    assert True


@pytest.mark.parametrize("mode", ["disabled", "offline"])
def test_close(api_token, mode):
    # given
    project = "workspace/project"

    # and
    run = Run(project=project, api_token=api_token, mode=mode)

    # when
    run.close()

    # then
    assert True


@pytest.mark.parametrize("mode", ["disabled", "offline"])
def test_run_id_too_long(api_token, mode):
    # given
    project = "workspace/project"

    # and
    run_id = "a" * 1000

    # then
    with pytest.raises(ValueError):
        with Run(project=project, api_token=api_token, run_id=run_id, mode=mode):
            ...

    # and
    assert True


@pytest.mark.parametrize("mode", ["disabled", "offline"])
def test_invalid_project_name(api_token, mode):
    # given

    # and
    project = "just-project"

    # then
    with pytest.raises(ValueError):
        with Run(project=project, api_token=api_token, mode=mode):
            ...

    # and
    assert True


@pytest.mark.parametrize("mode", ["disabled", "offline"])
def test_metadata(api_token, mode):
    # given
    project = "workspace/project"

    # then
    with Run(project=project, api_token=api_token, mode=mode) as run:
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


@pytest.mark.parametrize("mode", ["disabled", "offline"])
def test_tags(api_token, mode):
    # given
    project = "workspace/project"

    # then
    with Run(project=project, api_token=api_token, mode=mode) as run:
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


@pytest.mark.parametrize("mode", ["disabled", "offline"])
def test_log_without_step(api_token, mode):
    # given
    project = "workspace/project"

    # then
    with Run(project=project, api_token=api_token, mode=mode) as run:
        run.log(
            timestamp=datetime.now(),
            configs={
                "int": 1,
            },
        )

    # and
    assert True


@pytest.mark.parametrize("mode", ["disabled", "offline"])
def test_log_configs(api_token, mode):
    # given
    project = "workspace/project"

    # then
    with Run(project=project, api_token=api_token, mode=mode) as run:
        run.log_configs({"int": 1})
        run.log_configs({"float": 3.14})
        run.log_configs({"bool": True})
        run.log_configs({"string": "test"})
        run.log_configs({"datetime": datetime.now()})
        run.log_configs({"string_list": ["a", "b", "c"]})

    # and
    assert True


@pytest.mark.parametrize("mode", ["disabled", "offline"])
def test_log_step_float(api_token, mode):
    # given
    project = "workspace/project"

    # then
    with Run(project=project, api_token=api_token, mode=mode) as run:
        run.log(
            step=3.14,
            timestamp=datetime.now(),
            configs={
                "int": 1,
            },
        )

    # and
    assert True


@pytest.mark.parametrize("mode", ["disabled", "offline"])
def test_log_no_timestamp(api_token, mode):
    # given
    project = "workspace/project"

    # then
    with Run(project=project, api_token=api_token, mode=mode) as run:
        run.log(
            step=3.14,
            configs={
                "int": 1,
            },
        )

    # and
    assert True


@pytest.mark.parametrize("mode", ["disabled", "offline"])
def test_resume(api_token, mode):
    # given
    project = "workspace/project"

    # when
    with Run(project=project, api_token=api_token, resume=True, mode=mode) as run:
        run.log(
            step=3.14,
            configs={
                "int": 1,
            },
        )

    # then
    assert True


@freeze_time("2024-07-30 12:12:12.000022")
@pytest.mark.parametrize("mode", ["disabled", "offline"])
def test_creation_time(api_token, mode):
    # given
    project = "workspace/project"

    # when
    with Run(
        project=project,
        api_token=api_token,
        creation_time=datetime.now(),
        mode=mode,
    ):
        ...

    # then
    assert True


@pytest.mark.parametrize("mode", ["disabled", "offline"])
def test_assign_experiment(api_token, mode):
    # given
    project = "workspace/project"

    # when
    with Run(
        project=project,
        api_token=api_token,
        experiment_name="experiment_id",
        mode=mode,
    ):
        ...

    # then
    assert True


@pytest.mark.parametrize("mode", ["disabled", "offline"])
def test_forking(api_token, mode):
    # given
    project = "workspace/project"

    # when
    with Run(
        project=project,
        api_token=api_token,
        fork_run_id="parent-run-id",
        fork_step=3.14,
        mode=mode,
    ):
        ...

    # then
    assert True


@pytest.mark.parametrize("mode", ["disabled", "offline"])
def test_components_in_mode(api_token, mode):
    # given
    project = "workspace/project"

    # when
    with Run(
        project=project,
        api_token=api_token,
        mode=mode,
    ) as run:
        # then
        if mode == "disabled":
            assert run._attr_store is None
        else:
            assert run._attr_store is not None

        if mode in ("disabled", "offline"):
            assert run._sync_process is None
        else:
            assert run._sync_process is not None


@pytest.fixture
def temp_dir():
    temp_dir = tempfile.TemporaryDirectory()
    yield Path(temp_dir.name).resolve()

    try:
        temp_dir.cleanup()
    except Exception:
        # There are issues with windows workers: the temporary dir is being
        # held busy which results in an error during cleanup. We ignore these for now.
        if sys.platform != "win32":
            raise


@pytest.fixture
def mock_repo(monkeypatch, temp_dir):
    # Always switch to the temporary directory to avoid any side effects between tests.
    monkeypatch.chdir(temp_dir)

    with patch("neptune_scale.api.run.OperationsRepository", side_effect=OperationsRepository) as mock:
        yield mock


@pytest.mark.parametrize(
    ["log_dir_env", "log_dir_arg", "expected_path_relative_to_tmp_dir"],
    [
        (None, None, ""),
        ("from-env", None, "from-env"),
        (None, "from-arg", "from-arg"),
        ("from-env", "from-arg", "from-arg"),
    ],
)
def test_relative_run_log_directory(
    monkeypatch,
    temp_dir,
    mock_repo,
    api_token,
    log_dir_env,
    log_dir_arg,
    expected_path_relative_to_tmp_dir,
):
    monkeypatch.chdir(temp_dir)

    if log_dir_env is None:
        monkeypatch.delenv(envs.LOG_DIRECTORY, raising=False)
    else:
        monkeypatch.setenv(envs.LOG_DIRECTORY, log_dir_env)

    with Run(
        log_directory=log_dir_arg,
        project="workspace/project",
        api_token=api_token,
        mode="offline",
    ):
        ...

    mock_repo.assert_called_once()
    assert mock_repo.call_args[1]["db_path"].is_relative_to(temp_dir / expected_path_relative_to_tmp_dir)


@pytest.mark.parametrize(
    ["abs_log_dir_suffix_env", "abs_log_dir_suffix_arg", "expected_suffix"],
    [
        ("from-env", None, "from-env"),
        (None, "from-arg", "from-arg"),
        ("from-env", "from-arg", "from-arg"),
    ],
)
def test_absolute_run_log_directory(
    monkeypatch,
    temp_dir,
    mock_repo,
    api_token,
    abs_log_dir_suffix_env,
    abs_log_dir_suffix_arg,
    expected_suffix,
):
    if abs_log_dir_suffix_env is None:
        monkeypatch.delenv(envs.LOG_DIRECTORY, raising=False)
    else:
        monkeypatch.setenv(envs.LOG_DIRECTORY, str(Path(temp_dir / abs_log_dir_suffix_env).resolve()))

    with Run(
        log_directory=abs_log_dir_suffix_arg,
        project="workspace/project",
        api_token=api_token,
        mode="offline",
    ):
        ...

    mock_repo.assert_called_once()
    assert mock_repo.call_args[1]["db_path"].is_relative_to(temp_dir / expected_suffix)


@pytest.mark.parametrize(
    "project, run_id",
    [
        ("\t\nworkspace/\\project", "run\\-id\t\n12\x003"),
        ("workspace/project", "/run/with/slashes"),
        ("workspace/project", "\\run\\with\\backslashes"),
        ("workspace/漢字", "漢字"),
        ("w/" + "A" * 1000, "run-id"),
        ("workspace/project", "A" * 128),
        ("w/" + "A" * 1000, "A" * 128),
    ],
)
def test_run_with_ids_problematic_for_filesystems(api_token, project, run_id):
    """Test for potential problems with database files when using run ids that
    could be problematic for file systems"""

    with Run(
        project=project,
        api_token=api_token,
        run_id=run_id,
        mode="offline",
    ):
        ...
