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

from neptune_scale.api.run import print_message
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
        self._last_queued_seq_id: Optional[SequenceId] = None
        self._sync_process: Optional[SyncProcess] = None
        self._errors_monitor: Optional[ErrorsMonitor] = None

    def start(
        self,
    ) -> None:
        self._last_queued_seq_id = self._operations_repository.get_last_sequence_id()
        if self._last_queued_seq_id is None:
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
            mode="async",
            last_queued_seq=self._last_queued_seq,
            last_ack_seq=self._last_ack_seq,
            last_ack_timestamp=self._last_ack_timestamp,
        )
        self._errors_monitor = ErrorsMonitor(errors_queue=self._errors_queue)

        self._sync_process.start()
        self._process_link.start()

        self._errors_monitor.start()

    def wait(
        self,
        verbose: bool = True,
    ) -> None:
        if self._last_queued_seq_id is None:
            return

        if verbose:
            logger.info("Waiting for all operations to be processed")

        sleep_time: float = 1.0
        last_print_timestamp: Optional[float] = None

        while True:
            try:
                with self._last_ack_seq:
                    self._last_ack_seq.wait(timeout=sleep_time)
                    last_ack_seq_id = self._last_ack_seq.value

                if last_ack_seq_id == -1:
                    last_print_timestamp = print_message(
                        "Waiting. No operations were processed yet. Operations to sync: %s",
                        self._last_queued_seq_id + 1,
                        last_print=last_print_timestamp,
                        verbose=verbose,
                    )
                elif last_ack_seq_id < self._last_queued_seq_id:
                    last_print_timestamp = print_message(
                        "Waiting for remaining %d operation(s) to be processed",
                        self._last_queued_seq_id - last_ack_seq_id + 1,
                        last_print=last_print_timestamp,
                        verbose=verbose,
                    )
                elif last_ack_seq_id >= self._last_queued_seq_id:
                    break
            except KeyboardInterrupt:
                if verbose:
                    logger.warning("Waiting interrupted by user")
                return

        if verbose:
            logger.info("All operations were processed")

    def stop(self) -> None:
        if self._errors_monitor is not None:
            self._errors_monitor.interrupt()
            self._errors_monitor.join()

        if self._sync_process is not None:
            self._sync_process.terminate()
            self._sync_process.join()
            self._process_link.stop()

        self._operations_repository.close()
        self._errors_queue.close()
