import functools
import sys
import time
from dataclasses import (
    dataclass,
    field,
)
from pathlib import Path
from threading import (
    Condition,
    Event,
    Thread,
)
from typing import (
    Any,
    Optional,
    cast,
)

import click

from neptune_scale import Run
from neptune_scale.cli.util import (
    format_duration,
    format_local_run,
    is_neptune_dir,
)
from neptune_scale.exceptions import (
    NeptuneRunDuplicate,
    NeptuneSeriesStepNonIncreasing,
)
from neptune_scale.storage.operations import (
    OperationReader,
    OperationWriter,
    list_runs,
)
from neptune_scale.util import get_logger


@dataclass
class SyncState:
    allow_non_increasing_step: bool
    ignored_steps: int = 0

    run: Optional[Run] = None

    finished: Event = field(default_factory=Event)
    error: Optional[BaseException] = None

    cond: Condition = field(default_factory=Condition)

    # The next sequence number in the database to be marked as completed...
    db_seq: int = 0

    # ... as soon as we reach this operation sequence number in the run. This is the internal Run's sequence
    # counter managed in runtime only, ie. not stored in the database
    run_seq: int = -1

    def set_error(self, exc: BaseException) -> None:
        with self.cond:
            self.error = exc
            self.finished.set()
            self.cond.notify_all()

    def raise_if_error(self) -> None:
        with self.cond:
            if self.error:
                raise cast(Exception, self.error)


# Update the DB after each UPDATE_BATCH_SIZE operations submitted
UPDATE_BATCH_SIZE = 100

logger = get_logger()


# This function is run as a thread. Its purpose is to periodically mark operations that have been confirmed
# on the backend, as synced in the DB.
#
# The flow is as follows:
#
# 1. Read operations from the database in the main thread: _do_sync()
# 2. Every UPDATE_BATCH_SIZE operations signal this thread to wake up
# 3. The thread waits for the run to process submitted operations, up to `state.run_seq`,
#    which is the Run's internal, runtime-only sequence counter.
# 4. Once it happens, mark them as completed in the database, up to `state.db_seq`
def _db_updater_thread(project: str, run_id: str, db_path: Path, state: SyncState) -> None:
    assert state.run  # mypy

    writer: Optional[OperationWriter] = None
    try:
        writer = OperationWriter(project, run_id, db_path=db_path, resume=True)
        writer.init_db()

        last_db_seq = 0

        while True:
            with state.cond:
                while not state.cond.wait_for(lambda: state.db_seq > last_db_seq or state.finished.is_set()):
                    pass

                if state.error:
                    break

                db_seq, run_seq = state.db_seq, state.run_seq

            if db_seq != last_db_seq:
                while state.run._last_processed_operation_seq < run_seq:
                    # TODO: allow waiting up until a specifc run seq number, instead of waiting for all of them to be processed
                    # or just fix how `timeout` argument works.
                    state.run.wait_for_processing(timeout=1)

                # TODO: this is a hack to work around a race condition, where errors being pulled from the error
                # queue are reported after we've already marked the operations as synced. Without this, we would
                # mark them as synced even if they failed to process. This is temporary and needs to be solved
                # in a more reliable way.
                time.sleep(0.1)

                with state.cond:
                    if state.error:
                        break

                writer.mark_synced(db_seq)
                last_db_seq = db_seq

            # We might be signalled to finish work...
            if state.finished.is_set():
                with state.cond:
                    # ... but there could be as last batch of operations to confirm, so only break
                    # once we're in sync, or on error
                    if state.db_seq == last_db_seq or state.error:
                        break
    except Exception as exc:
        state.set_error(exc)

    if writer:
        writer.close()


def _error_callback(state: SyncState, exc: BaseException, ts: Optional[float]) -> None:
    if state.allow_non_increasing_step and isinstance(exc, NeptuneSeriesStepNonIncreasing):
        with state.cond:
            state.ignored_steps += 1
        return

    state.set_error(exc)


def _warning_callback(state: SyncState, exc: BaseException, ts: Optional[float]) -> None:
    if isinstance(exc, NeptuneRunDuplicate):
        # Silence the warning
        return
    # elif isinstance(exc, NeptuneRunForkParentNotFound):
    #     state.set_error(exc)
    #     return

    logger.warning(f"{exc}")


def _do_sync(reader: OperationReader, state: SyncState) -> None:
    # While submitting operations keep track of how many we've already submitted.
    # Periodically signal the updater thread every N operations to wait for them to be processed,
    # and mark them in the database as synced. See the comment at _updater_thread() for more details.
    operation_count = 0
    t0 = time.monotonic()
    db_seq = 0
    run_seq = -1

    assert state.run  # mypy

    for op in reader.pending_operations:
        run_seq = state.run._attr_store.log_raw(op.data, op.seq)
        db_seq = op.seq

        operation_count += 1
        now = time.monotonic()

        # Signal the DB updater thread
        if operation_count % UPDATE_BATCH_SIZE == 0 or now - t0 > 1.0:
            t0 = now

            with state.cond:
                state.raise_if_error()

                state.db_seq, state.run_seq = op.seq, run_seq
                state.cond.notify_all()

            state.raise_if_error()

    # Signal the remaining batch of operations submitted
    with state.cond:
        state.db_seq, state.run_seq = db_seq, run_seq
        state.finished.set()
        state.cond.notify_all()


def sync_file(path: Path, allow_non_increasing_step: bool) -> None:
    logger.info(f"Processing file {path}")
    reader = OperationReader(path)
    local_run = reader.run

    logger.info(format_local_run(local_run))

    if reader.pending_operations_count == 0:
        logger.info("No operations to sync")
        return

    resume = local_run.last_synced_operation > 0
    if resume:
        logger.info("Resuming sync")
        extra_kwargs: dict[str, Any] = {}
    else:
        extra_kwargs = dict(
            experiment_name=local_run.experiment_name,
            fork_run_id=local_run.fork_run_id,
            fork_step=local_run.fork_step,
            creation_time=local_run.creation_time,
        )

    state = SyncState(allow_non_increasing_step)
    run = Run(
        run_id=local_run.run_id,
        project=local_run.project,
        resume=resume,
        on_warning_callback=functools.partial(_warning_callback, state),
        on_error_callback=functools.partial(_error_callback, state),
        **extra_kwargs,
    )
    state.run = run

    updater = Thread(target=_db_updater_thread, args=(local_run.project, local_run.run_id, local_run.path, state))
    updater.start()

    try:
        _do_sync(reader, state)
    finally:
        updater.join()

    run.close()

    if state.error:
        raise state.error

    if state.ignored_steps:
        logger.info(f"Ignored {state.ignored_steps} non-increasing steps")


@click.command()
@click.argument("filename", required=False, type=click.Path(exists=True, dir_okay=False))
@click.option("-k", "--keep", is_flag=True, help="Do not delete the local copy of the data after sync")
@click.option(
    "--allow-non-increasing-step",
    is_flag=True,
    help="Do not abort on non-increasing metric steps being sent. This is useful for resuming interrupted syncs that "
    "are stuck on a metric being sent multiple times. ",
)
@click.pass_context
def sync(ctx: click.Context, filename: Optional[str], keep: bool, allow_non_increasing_step: bool) -> None:
    neptune_dir = ctx.obj["neptune_dir"]
    if not is_neptune_dir(neptune_dir):
        logger.error(f"No Neptune data found at {neptune_dir}")
        sys.exit(1)

    t0 = time.monotonic()
    if filename:
        files = [Path(filename)]
    else:
        files = [run.path for run in list_runs(ctx.obj["neptune_dir"])]

    if not files:
        logger.info("No data to sync")
        sys.exit(1)

    logger.info("Starting `neptune sync`")

    error = False

    for path in files:
        try:
            sync_file(path, allow_non_increasing_step=allow_non_increasing_step)
            if not keep:
                logger.info(f"Removing file {path}")
                path.unlink()
        except Exception as e:
            logger.error(e)
            error = True

    duration = format_duration(int(time.monotonic() - t0))
    if error:
        logger.error(f"Sync finished in {duration} with errors")
        sys.exit(-1)
    else:
        logger.info(f"Sync finished in {duration} successfully")
