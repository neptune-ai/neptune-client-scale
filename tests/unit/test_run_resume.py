import time

from neptune_scale import Run
from neptune_scale.api.run import generate_run_id


def test_resume_false_with_matching_fork_point(api_token, caplog):
    project = "workspace/project"
    run_id = generate_run_id()
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

    time.sleep(0.01)
    # Then try to create the same run again without resume
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

    time.sleep(0.01)
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


def test_resume_true(
    api_token,
):
    project = "workspace/project"
    run_id = generate_run_id()
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
    time.sleep(0.01)
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
    run_id = generate_run_id()

    # First create a run with one fork point
    with Run(project=project, api_token=api_token, run_id=run_id, mode="offline"):
        pass

    # Then resume the run with a different fork point
    time.sleep(0.01)
    with Run(project=project, api_token=api_token, run_id=run_id, resume=True, mode="offline"):
        pass


def test_resume_true_with_no_metadata(
    api_token,
):
    project = "workspace/project"

    # Create a run with resume=True but no pre-existing metadata
    with Run(project=project, api_token=api_token, resume=True, mode="offline"):
        pass
