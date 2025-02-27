import uuid

import pytest

from neptune_scale import Run
from neptune_scale.exceptions import (
    NeptuneRunConflicting,
    NeptuneRunDuplicate,
)


def test_resume_false_with_matching_fork_point(api_token):
    project = "workspace/project"
    run_id = str(uuid.uuid4())
    fork_run_id = "parent-run"
    fork_step = 5

    # First create a run to set up the metadata
    with Run(
        project=project,
        api_token=api_token,
        run_id=run_id,
        mode="disabled",
        fork_run_id=fork_run_id,
        fork_step=fork_step,
    ):
        pass

    # Then try to create the same run again without resume
    with pytest.raises(NeptuneRunDuplicate):
        Run(
            project=project,
            api_token=api_token,
            run_id=run_id,
            resume=False,
            mode="disabled",
            fork_run_id=fork_run_id,
            fork_step=fork_step,
        )

    # Then try to use the same run_id with a different project
    with Run(
        project=project + "2",
        api_token=api_token,
        run_id=run_id,
        resume=False,
        mode="disabled",
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
        project=project, api_token=api_token, run_id=run_id, mode="disabled", fork_run_id="parent-run-1", fork_step=5
    ):
        pass

    # Then try to create the same run but with a different fork point
    with pytest.raises(NeptuneRunConflicting):
        Run(
            project=project,
            api_token=api_token,
            run_id=run_id,
            resume=False,
            mode="disabled",
            fork_run_id="parent-run-2",
            fork_step=10,
        )

    # Then try to create the same run but with a different run_id
    with Run(
        project=project + "2",
        api_token=api_token,
        run_id=run_id,
        resume=False,
        mode="disabled",
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
        mode="disabled",
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
        mode="disabled",
    ):
        pass


def test_resume_true_without_fork_point(
    api_token,
):
    project = "workspace/project"
    run_id = str(uuid.uuid4())

    # First create a run with one fork point
    with Run(project=project, api_token=api_token, run_id=run_id, mode="disabled"):
        pass

    # Then resume the run with a different fork point
    with Run(project=project, api_token=api_token, run_id=run_id, resume=True, mode="disabled"):
        pass


def test_resume_true_with_no_metadata(
    api_token,
):
    project = "workspace/project"
    run_id = str(uuid.uuid4())

    # Create a run with resume=True but no pre-existing metadata
    with Run(project=project, api_token=api_token, run_id=run_id, resume=True, mode="disabled"):
        pass
