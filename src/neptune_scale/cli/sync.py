#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

__all__ = ["sync_all"]

from pathlib import Path
from typing import Optional

from tqdm import tqdm

from neptune_scale.sync import sync_process
from neptune_scale.sync.errors_tracking import (
    ErrorsMonitor,
    ErrorsQueue,
)
from neptune_scale.sync.operations_repository import (
    OperationsRepository,
    SequenceId,
)
from neptune_scale.sync.sync_process import SyncProcess
from neptune_scale.util import (
    SharedFloat,
    SharedInt,
    get_logger,
)

logger = get_logger()


def sync_all(
    run_log_file: Path,
    api_token: str,
) -> None:
    if not run_log_file.exists():
        raise FileNotFoundError(f"Run log file {run_log_file} does not exist")
    run_log_file = run_log_file.resolve()

    runner = SyncRunner(api_token=api_token, run_log_file=run_log_file)

    try:
        runner.start()
        runner.wait()
    finally:
        runner.stop()


class SyncRunner:
    def __init__(
        self,
        api_token: str,
        run_log_file: Path,
    ) -> None:
        self._api_token: str = api_token
        self._run_log_file: Path = run_log_file
        self._operations_repository: OperationsRepository = OperationsRepository(db_path=run_log_file)
        self._process_link = sync_process.ProcessLink()
        self._errors_queue: ErrorsQueue = ErrorsQueue()
        self._last_queued_seq = SharedInt(-1)
        self._last_ack_seq = SharedInt(-1)
        self._last_ack_timestamp = SharedFloat(-1)
        self._log_seq_id_range: Optional[tuple[SequenceId, SequenceId]] = None
        self._sync_process: Optional[SyncProcess] = None
        self._errors_monitor: Optional[ErrorsMonitor] = None

    def start(
        self,
    ) -> None:
        self._log_seq_id_range = self._operations_repository.get_sequence_id_range()
        if self._log_seq_id_range is None:
            logger.info("No operations to process")
            return

        metadata = self._operations_repository.get_metadata()
        if metadata is None:
            logger.error("No run metadata found in log")
            return

        self._sync_process = sync_process.SyncProcess(
            operations_repository_path=self._run_log_file,
            errors_queue=self._errors_queue,
            process_link=self._process_link,
            api_token=self._api_token,
            project=metadata.project,
            family=metadata.run_id,
            last_queued_seq=self._last_queued_seq,
            last_ack_seq=self._last_ack_seq,
            last_ack_timestamp=self._last_ack_timestamp,
        )
        self._errors_monitor = ErrorsMonitor(errors_queue=self._errors_queue)

        self._sync_process.start()
        self._process_link.start()

        self._errors_monitor.start()

    def wait(self, progress_bar_enabled: bool = True, wait_time: float = 0.1) -> None:
        if self._log_seq_id_range is None:
            return

        total_count = self._log_seq_id_range[1] - self._log_seq_id_range[0] + 1
        with tqdm(
            desc="Syncing operations", total=total_count, unit="op", disable=not progress_bar_enabled
        ) as progress_bar:
            while True:
                try:
                    if self._sync_process is None or not self._sync_process.is_alive():
                        logger.warning("Waiting interrupted because sync process is not running")
                        return

                    with self._last_ack_seq:
                        self._last_ack_seq.wait(timeout=wait_time)
                        last_ack_seq_id = self._last_ack_seq.value

                    if last_ack_seq_id != -1:
                        acked_count = last_ack_seq_id - self._log_seq_id_range[0] + 1
                        progress_bar.update(acked_count - progress_bar.n)

                    if last_ack_seq_id >= self._log_seq_id_range[1]:
                        break
                except KeyboardInterrupt:
                    logger.warning("Waiting interrupted by user")
                    return

    def stop(self) -> None:
        if self._errors_monitor is not None:
            self._errors_monitor.interrupt()
            self._errors_monitor.join()

        if self._sync_process is not None:
            self._sync_process.terminate()
            self._sync_process.join()
            self._process_link.stop()

        self._operations_repository.close(cleanup_files=True)
        self._errors_queue.close()
