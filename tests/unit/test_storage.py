import tempfile
import uuid
from datetime import (
    datetime,
    timezone,
)
from pathlib import Path

import pytest
from pytest import fixture

from neptune_scale.storage.operations import (
    OperationReader,
    OperationWriter,
    database_path_for_run,
    init_write_storage,
)
from neptune_scale.sync.metadata_splitter import MetadataSplitter


@fixture(scope="module")
def project():
    return "workspace/project"


@fixture
def run_id():
    return str(uuid.uuid4())


@fixture
def temp_db_dir():
    with tempfile.TemporaryDirectory() as tmp:
        yield Path(tmp)


@fixture
def db_path(temp_db_dir, project, run_id):
    return Path(temp_db_dir) / database_path_for_run(project, run_id)


@fixture
def writer(project, run_id, temp_db_dir, db_path):
    path = init_write_storage(project, run_id, str(temp_db_dir))
    assert path == db_path

    writer = OperationWriter(project, run_id, db_path)
    writer.init_db()
    yield writer
    writer.close()


def _make_operations(project, run_id, nsteps=10):
    result = []
    for step in range(nsteps):
        splitter = MetadataSplitter(
            project=project,
            run_id=run_id,
            timestamp=datetime.now(),
            step=step,
            configs={"int-atom": step**2},
            metrics={"series": step**0.5},
            add_tags={"tags": [f"tag-{step}"]},
            remove_tags={"tags": [f"tag-{step + nsteps}"]},
        )

        for operation, _size in splitter:
            result.append(operation)

    return result


def _verify_local_run(
    reader,
    *,
    project,
    run_id,
    pending_operations_count,
    completed_operations_count,
    creation_time,
    experiment_name,
    fork_run_id,
    fork_step,
):
    run = reader.run
    assert reader.pending_operations_count == pending_operations_count
    assert reader.completed_operations_count == completed_operations_count
    assert run.project == project
    assert run.run_id == run_id
    assert run.operation_count == 0
    assert run.creation_time == creation_time
    assert run.last_synced_operation == 0
    assert run.experiment_name == experiment_name
    assert run.fork_run_id == fork_run_id
    assert run.fork_step == fork_step


def test_init_write_storage(project, run_id, temp_db_dir):
    now = datetime.now(timezone.utc)
    experiment_name = "the-experiment"
    fork_run_id = "forked-run-id"
    fork_step = 13.37

    path = init_write_storage(
        project,
        run_id,
        base_dir=str(temp_db_dir),
        creation_time=now,
        experiment_name=experiment_name,
        fork_run_id=fork_run_id,
        fork_step=fork_step,
    )

    assert path.is_relative_to(temp_db_dir)

    reader = OperationReader(path)
    _verify_local_run(
        reader,
        project=project,
        run_id=run_id,
        pending_operations_count=0,
        completed_operations_count=0,
        creation_time=now,
        experiment_name=experiment_name,
        fork_run_id=fork_run_id,
        fork_step=fork_step,
    )


def test_write_and_read_consistency(project, run_id, temp_db_dir):
    now = datetime.now(timezone.utc)
    experiment_name = "the-experiment"
    fork_run_id = "forked-run-id"
    fork_step = 13.37

    path = init_write_storage(
        project,
        run_id,
        base_dir=str(temp_db_dir),
        creation_time=now,
        experiment_name=experiment_name,
        fork_run_id=fork_run_id,
        fork_step=fork_step,
    )

    writer = OperationWriter(project, run_id, path)
    writer.init_db()

    ops = _make_operations(project, run_id)
    for op in ops:
        writer.write(op.SerializeToString())

    writer.close()

    # Writing ops shouldn't break run metadata
    reader = OperationReader(path)
    _verify_local_run(
        reader,
        project=project,
        run_id=run_id,
        pending_operations_count=len(ops),
        completed_operations_count=0,
        creation_time=now,
        experiment_name=experiment_name,
        fork_run_id=fork_run_id,
        fork_step=fork_step,
    )

    for seq, (written_op, read_op) in enumerate(zip(ops, reader.pending_operations), start=1):
        assert read_op.data == written_op.SerializeToString()
        assert read_op.seq == seq
        assert read_op.run_id == run_id


def test_reader_file_does_not_exist(temp_db_dir):
    with pytest.raises(FileNotFoundError) as exc:
        OperationReader(temp_db_dir / "non-existent-file")

    exc.match("Database not found")
    exc.match(str(temp_db_dir))


def test_mark_as_synced(project, run_id, db_path, writer):
    ops = _make_operations(project, run_id)
    for op in ops:
        writer.write(op.SerializeToString())
    writer.commit()
