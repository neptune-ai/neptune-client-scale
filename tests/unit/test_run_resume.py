import uuid

import pytest

from neptune_scale import Run
from neptune_scale.api.run import _validate_existing_db
from neptune_scale.exceptions import (
    NeptuneConflictingDataInLocalStorage,
    NeptuneLocalStorageInUnsupportedVersion,
)
from neptune_scale.sync.operations_repository import Metadata


def test_resume_false_with_matching_fork_point(api_token, caplog):
    project = "workspace/project"
    run_id = str(uuid.uuid4())
    fork_run_id = "parent-run"
    fork_step = 5

    # First create a run to set up the metadata
    with Run(
        project=project,
        api_token=api_token,
        run_id=run_id,
        mode="offline",
        fork_run_id=fork_run_id,
        fork_step=fork_step,
    ):
        pass

    # Then try to create the same run again without resume
    with caplog.at_level("WARNING"):
        with Run(
            project=project,
            api_token=api_token,
            run_id=run_id,
            resume=False,
            mode="offline",
            fork_run_id=fork_run_id,
            fork_step=fork_step,
        ):
            pass
    assert "Run already exists in local storage" in caplog.text

    # Then try to use the same run_id with a different project
    with Run(
        project=project + "2",
        api_token=api_token,
        run_id=run_id,
        resume=False,
        mode="offline",
        fork_run_id=fork_run_id,
        fork_step=fork_step,
    ):
        pass


def test_resume_false_with_conflicting_fork_point(
    api_token,
):
    project = "workspace/project"
    run_id = str(uuid.uuid4())

    # First create a run with one fork point
    with Run(
        project=project, api_token=api_token, run_id=run_id, mode="offline", fork_run_id="parent-run-1", fork_step=5
    ):
        pass

    # Then try to create the same run but with a different fork point
    with pytest.raises(NeptuneConflictingDataInLocalStorage):
        Run(
            project=project,
            api_token=api_token,
            run_id=run_id,
            resume=False,
            mode="offline",
            fork_run_id="parent-run-2",
            fork_step=10,
        )

    # Then try to create the same run but with a different run_id
    with Run(
        project=project + "2",
        api_token=api_token,
        run_id=run_id,
        resume=False,
        mode="offline",
        fork_run_id="parent-run-2",
        fork_step=10,
    ):
        pass


def test_resume_true(
    api_token,
):
    project = "workspace/project"
    run_id = str(uuid.uuid4())
    fork_run_id = "parent-run"
    fork_step = 5.0

    # First create a run to set up the metadata
    with Run(
        project=project,
        api_token=api_token,
        run_id=run_id,
        mode="offline",
        fork_run_id=fork_run_id,
        fork_step=fork_step,
    ):
        pass

    # Then resume the same run with matching fork point

    with Run(
        project=project,
        api_token=api_token,
        run_id=run_id,
        resume=True,
        mode="offline",
    ):
        pass


def test_resume_true_without_fork_point(
    api_token,
):
    project = "workspace/project"
    run_id = str(uuid.uuid4())

    # First create a run with one fork point
    with Run(project=project, api_token=api_token, run_id=run_id, mode="offline"):
        pass

    # Then resume the run with a different fork point
    with Run(project=project, api_token=api_token, run_id=run_id, resume=True, mode="offline"):
        pass


def test_resume_true_with_no_metadata(
    api_token,
):
    project = "workspace/project"
    run_id = str(uuid.uuid4())

    # Create a run with resume=True but no pre-existing metadata
    with Run(project=project, api_token=api_token, run_id=run_id, resume=True, mode="offline"):
        pass


def test_unsupported_version_error():
    # Given - create a metadata with an unsupported version
    metadata = Metadata(
        version="unsupported_version", project="project", run_id="run_id", parent_run_id="parent_run_id", fork_step=1.0
    )

    with pytest.raises(NeptuneLocalStorageInUnsupportedVersion):
        _validate_existing_db(
            metadata, resume=False, project="project", run_id="run_id", fork_run_id="parent_run_id", fork_step=1.0
        )
