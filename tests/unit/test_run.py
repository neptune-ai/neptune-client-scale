import sys
import tempfile
from dataclasses import dataclass
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
def test_log_configs_cast_unsupported(api_token, mode):
    # given
    from datetime import datetime

    project = "workspace/project"

    class Custom:
        def __str__(self):
            return "custom_obj"

    @dataclass
    class CustomDataclass:
        a: int
        b: str
        c: dict[str, int]
        d: None

    custom_dataclass = CustomDataclass(a=1, b="abc", c={"a": 1, "b": 2}, d=None)

    now = datetime.now()

    # then
    with (
        Run(project=project, api_token=api_token, mode=mode) as run,
        patch.object(run, "_log") as mock_log,
    ):
        # Supported types
        configs = {
            "int": 1,
            "float": 2.5,
            "bool": False,
            "str": "abc",
            "datetime": now,
        }
        run.log_configs(configs, cast_unsupported=True)
        mock_log.assert_called_with(configs=configs)
        mock_log.reset_mock()

        # None becomes ""
        run.log_configs({"none": None}, cast_unsupported=True)
        mock_log.assert_called_with(configs={"none": ""})
        mock_log.reset_mock()

        # Homogeneous string list/set/tuple are preserved
        run.log_configs({"str_list": ["a", "b"]}, cast_unsupported=True)
        mock_log.assert_called_with(configs={"str_list": ["a", "b"]})
        run.log_configs({"str_set": {"a", "b"}}, cast_unsupported=True)
        mock_log.assert_called_with(configs={"str_set": {"a", "b"}})
        run.log_configs({"str_tuple": ("a", "b")}, cast_unsupported=True)
        mock_log.assert_called_with(configs={"str_tuple": ("a", "b")})
        mock_log.reset_mock()

        # Mixed or non-string list/set/tuple are cast entirely
        run.log_configs({"mixed_list": [1, "b", None]}, cast_unsupported=True)
        mock_log.assert_called_with(configs={"mixed_list": "[1, 'b', None]"})
        run.log_configs({"mixed_tuple": (1, "b")}, cast_unsupported=True)
        mock_log.assert_called_with(configs={"mixed_tuple": "(1, 'b')"})
        mock_log.reset_mock()

        # Custom object becomes str
        run.log_configs({"custom": Custom()}, cast_unsupported=True)
        mock_log.assert_called_with(configs={"custom": "custom_obj"})
        mock_log.reset_mock()

        # Dict becomes str
        run.log_configs({"dict": {"a": 1}}, cast_unsupported=True)
        mock_log.assert_called_with(configs={"dict": "{'a': 1}"})
        mock_log.reset_mock()

        # Custom dataclass becomes str
        run.log_configs(custom_dataclass, cast_unsupported=True)
        mock_log.assert_called_with(configs={"a": 1, "b": "abc", "c": "{'a': 1, 'b': 2}", "d": ""})
        mock_log.reset_mock()

    # and
    assert True


@pytest.mark.parametrize("mode", ["disabled", "offline"])
def test_log_configs_flatten(api_token, mode):
    # given
    project = "workspace/project"

    @dataclass
    class CustomDataclass:
        a: int
        b: str
        c: dict[str, int]
        d: None

    custom_dataclass = CustomDataclass(a=1, b="abc", c={"a": 1, "b": 2}, d=None)

    # then
    with (
        Run(project=project, api_token=api_token, mode=mode) as run,
        patch.object(run, "_log") as mock_log,
    ):
        run.log_configs({"a": {"b": 1, "c": {"d": 2}}}, flatten=True)
        mock_log.assert_called_with(configs={"a/b": 1, "a/c/d": 2})
        mock_log.reset_mock()

        # Nested dataclass becomes dict
        run.log_configs(custom_dataclass, flatten=True)
        mock_log.assert_called_with(configs={"a": 1, "b": "abc", "c/a": 1, "c/b": 2, "d": None})
        mock_log.reset_mock()

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
            assert run._logging_enabled == False
        else:
            assert run._logging_enabled == True

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


def test_run_cleans_up_empty_storage_directory(mock_repo, temp_dir, api_token):
    with Run(log_directory=temp_dir, project="workspace/project", api_token=api_token, mode="offline") as run:
        # Pretend all operations are completed, so that the DB is deleted on close,
        # otherwise the dir will not be empty and will not be removed
        run._operations_repo.delete_operations(100)

    # temp_dir should be empty
    assert not list(temp_dir.iterdir()), "Run directory was not removed"


def test_run_does_not_clean_up_non_empty_storage_directory(mock_repo, temp_dir, api_token):
    with Run(log_directory=temp_dir, project="workspace/project", api_token=api_token, mode="offline") as run:
        with open(run._storage_directory_path / "file.txt", "w") as f:
            f.write("test")

    # A single directory should exist in temp_dir, one for the Run above
    assert len(list(temp_dir.iterdir())) == 1
    assert run._storage_directory_path.exists()


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


@pytest.mark.parametrize("enable_console_log_capture", [True, False])
def test_run_disable_console_log_capture(api_token, enable_console_log_capture):
    with Run(project="a/b", mode="offline", enable_console_log_capture=enable_console_log_capture) as run:
        if enable_console_log_capture:
            assert run._console_log_capture is not None
        else:
            assert run._console_log_capture is None


@pytest.mark.parametrize(
    "runtime_namespace, expected_path",
    [
        (None, "runtime"),
        ("custom/namespace", "custom/namespace"),
        ("custom/namespace/", "custom/namespace"),
    ],
)
def test_run_runtime_namespace(runtime_namespace, expected_path):
    with Run(
        project="workspace/project",
        mode="offline",
        enable_console_log_capture=True,
        runtime_namespace=runtime_namespace,
    ) as run:
        assert run._console_log_capture._stdout_attribute == f"{expected_path}/stdout"
        assert run._console_log_capture._stderr_attribute == f"{expected_path}/stderr"
