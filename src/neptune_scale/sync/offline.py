import queue
from datetime import datetime
from typing import (
    Optional,
    Union,
)

from neptune_scale.exceptions import NeptuneScaleError
from neptune_scale.storage.operations import (
    OperationWriter,
    database_path_for_run,
    init_write_storage,
)
from neptune_scale.sync.errors_tracking import ErrorsQueue
from neptune_scale.sync.queue_element import SingleOperation
from neptune_scale.util import (
    Daemon,
    SharedInt,
    envs,
)
from neptune_scale.util.abstract import Resource


def init_offline_mode(
    project: str,
    run_id: str,
    resume: bool,
    *,
    creation_time: Optional[datetime] = None,
    experiment_name: Optional[str] = None,
    fork_run_id: Optional[str] = None,
    fork_step: Optional[Union[int, float]] = None,
) -> None:
    """Called by the main process, Run.__init__()"""

    base_dir = envs.get_str(envs.BASE_STORAGE_DIR)
    path = database_path_for_run(project, run_id, base_dir)

    if not resume:
        if path.exists():
            raise NeptuneScaleError(
                reason=f"Offline Run `{run_id}` already exists at `{path}`. Use `resume=True` to continue."
            )
    else:
        if not path.exists():
            raise NeptuneScaleError(
                reason=f"Unable to resume offline Run `{run_id}`: local data does not exist at `{path}`."
            )

    init_write_storage(
        project,
        run_id,
        base_dir,
        creation_time=creation_time,
        experiment_name=experiment_name,
        fork_run_id=fork_run_id,
        fork_step=fork_step,
    )


class OfflineModeWriterThread(Daemon, Resource):
    """
    Fed by OperationDispatcherThread via `input_queue`, writes operations to the local store.
    """

    def __init__(
        self,
        store: OperationWriter,
        input_queue: queue.Queue[SingleOperation],
        last_ack_seq: SharedInt,
        errors_queue: ErrorsQueue,
    ) -> None:
        super().__init__(name="OfflineModeWorkerThread", sleep_time=1)

        self._store = store
        self._input_queue = input_queue
        self._last_ack_seq = last_ack_seq
        self._errors_queue = errors_queue

    def run(self) -> None:
        try:
            self._store.init_db()
        except Exception as e:
            self._errors_queue.put(e)
            self.interrupt()
            return

        super().run()

    def work(self) -> None:
        while self.is_running():
            try:
                msg = self._input_queue.get(block=False)
                self._store.write(msg.operation)

                with self._last_ack_seq:
                    self._last_ack_seq.value = msg.sequence_id
                    self._last_ack_seq.notify_all()
            except queue.Empty:
                continue
            except Exception as e:
                self._errors_queue.put(e)
                self.interrupt()
                break

        self.close()

    def close(self) -> None:
        self._store.close()
