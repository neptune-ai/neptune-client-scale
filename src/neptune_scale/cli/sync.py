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

import time
from dataclasses import dataclass
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


@dataclass
class _ProgressStatus:
    progress: int
    max_progress: int

    @property
    def finished(self) -> bool:
        return self.progress >= self.max_progress

    def updated(self, progress: int) -> "_ProgressStatus":
        return _ProgressStatus(
            progress=progress,
            max_progress=self.max_progress,
        )


class SyncRunner:
    def __init__(
        self,
        api_token: str,
        run_log_file: Path,
    ) -> None:
        self._api_token: str = api_token
        self._run_log_file: Path = run_log_file
        self._operations_repository: OperationsRepository = OperationsRepository(db_path=run_log_file)
        self._errors_queue: ErrorsQueue = ErrorsQueue()
        self._last_queued_seq = SharedInt(-1)
        self._last_ack_seq = SharedInt(-1)
        self._last_ack_timestamp = SharedFloat(-1)
        self._log_seq_id_range: Optional[tuple[SequenceId, SequenceId]] = None
        self._file_upload_request_init_count: Optional[int] = None
        self._sync_process: Optional[SyncProcess] = None
        self._errors_monitor: Optional[ErrorsMonitor] = None

    def start(
        self,
    ) -> None:
        self._log_seq_id_range = self._operations_repository.get_sequence_id_range()
        self._file_upload_request_init_count = self._operations_repository.get_file_upload_requests_count()

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
            api_token=self._api_token,
            project=metadata.project,
            family=metadata.run_id,
            last_queued_seq=self._last_queued_seq,
            last_ack_seq=self._last_ack_seq,
            last_ack_timestamp=self._last_ack_timestamp,
        )
        self._errors_monitor = ErrorsMonitor(errors_queue=self._errors_queue)

        self._sync_process.start()

        self._errors_monitor.start()

    def wait(self, progress_bar_enabled: bool = True, wait_time: float = 0.1) -> None:
        operation_progress = _ProgressStatus(
            progress=0,
            max_progress=self._log_seq_id_range[1] - self._log_seq_id_range[0] + 1 if self._log_seq_id_range else 0,
        )
        file_progress = _ProgressStatus(progress=0, max_progress=self._file_upload_request_init_count or 0)

        if operation_progress.finished and file_progress.finished:
            return

        with tqdm(
            desc="Syncing operations",
            total=operation_progress.max_progress + file_progress.max_progress,
            unit="op",
            disable=not progress_bar_enabled,
        ) as progress_bar:
            while True:
                try:
                    if self._sync_process is None or not self._sync_process.is_alive():
                        logger.warning("Waiting interrupted because sync process is not running")
                        return

                    operation_progress = self._wait_operation_submit(
                        last_progress=operation_progress, wait_time=wait_time
                    )
                    file_progress = self._wait_file_upload(last_progress=file_progress, wait_time=wait_time)

                    progress_bar.update(operation_progress.progress + file_progress.progress - progress_bar.n)

                    if operation_progress.finished and file_progress.finished:
                        return

                except KeyboardInterrupt:
                    logger.warning("Waiting interrupted by user")
                    return

    def _wait_operation_submit(self, last_progress: _ProgressStatus, wait_time: float) -> _ProgressStatus:
        if last_progress.finished:
            return last_progress
        assert self._log_seq_id_range is not None

        with self._last_ack_seq:
            self._last_ack_seq.wait(timeout=wait_time)
            last_ack_seq_id = self._last_ack_seq.value

        if last_ack_seq_id != -1:
            acked_count = last_ack_seq_id - self._log_seq_id_range[0] + 1
        else:
            acked_count = 0

        return last_progress.updated(progress=acked_count)

    def _wait_file_upload(self, last_progress: _ProgressStatus, wait_time: float) -> _ProgressStatus:
        if last_progress.finished:
            return last_progress
        assert self._file_upload_request_init_count is not None

        upload_request_count = self._operations_repository.get_file_upload_requests_count()
        uploaded_count = self._file_upload_request_init_count - upload_request_count
        if upload_request_count > 0:
            time.sleep(wait_time)

        return last_progress.updated(progress=uploaded_count)

    def stop(self) -> None:
        if self._sync_process is not None:
            self._sync_process.terminate()
            self._sync_process.join()

        if self._errors_monitor is not None:
            self._errors_monitor.interrupt(remaining_iterations=1)
            self._errors_monitor.join()

        self._operations_repository.close(cleanup_files=True)
        self._errors_queue.close()
