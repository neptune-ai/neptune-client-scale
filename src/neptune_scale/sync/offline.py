import queue

from neptune_scale.sync.errors_tracking import ErrorsQueue
from neptune_scale.sync.queue_element import SingleOperation
from neptune_scale.sync.storage.operations import (
    OperationWriter,
    database_path_for_run,
    init_storage,
)
from neptune_scale.util import (
    Daemon,
    SharedInt,
    envs,
)
from neptune_scale.util.abstract import Resource


def init_offline_mode(project: str, run_id: str, resume: bool) -> None:
    """Called by the main process, Run.__init__()"""

    base_dir = envs.get_str(envs.BASE_STORAGE_DIR)

    # TODO: support resume=True in Run.__init__()
    # if not resume and (path := database_path_for_run(project, run_id, base_dir)).exists():
    #    raise ValueError(f"Offline Run {run_id} already exists at {path}. Use `resume=True` to continue.")

    if (path := database_path_for_run(project, run_id, base_dir)).exists():
        raise ValueError(f"Data for offline Run `{run_id}` already exists at `{path}`")

    init_storage(project, run_id, base_dir)


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
        self._store.init_db()

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
