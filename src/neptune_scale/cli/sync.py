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
from neptune_scale.sync.operations_repository import OperationsRepository
from neptune_scale.sync.sequence_tracker import SequenceTracker
from neptune_scale.util import (
    Daemon,
    SharedFloat,
    SharedInt,
    get_logger,
)

logger = get_logger()


def sync_all(
    run_log_file: Path,
    api_token: str,
) -> None:
    repository = OperationsRepository(db_path=run_log_file)

    runner = SyncRunner(api_token=api_token, operations_repository=repository)

    try:
        runner.start()
        runner.wait()
    finally:
        runner.stop()


class SyncRunner:
    def __init__(
        self,
        api_token: str,
        operations_repository: OperationsRepository,
    ) -> None:
        self._api_token: str = api_token
        self._operations_repository: OperationsRepository = operations_repository
        self._status_tracking_queue: sync_process.PeekableQueue[sync_process.StatusTrackingElement] = (
            sync_process.PeekableQueue()
        )
        self._errors_queue: ErrorsQueue = ErrorsQueue()
        self._last_queued_seq = SharedInt(-1)
        self._last_ack_seq = SharedInt(-1)
        self._last_ack_timestamp = SharedFloat(-1)
        self._sequence_tracker = SequenceTracker()
        self.threads: list[Daemon] = []

    def start(
        self,
    ) -> None:
        last_sequence_id = self._operations_repository.get_last_sequence_id()
        if last_sequence_id is not None:
            self._sequence_tracker.update_sequence_id(last_sequence_id)
        else:
            logger.info("No operations to process")
            return

        metadata = self._operations_repository.get_metadata()
        if metadata is None:
            logger.error("No run metadata found in log")
            return

        self.threads = [
            sync_process.SenderThread(
                api_token=self._api_token,
                mode="async",
                operations_repository=self._operations_repository,
                status_tracking_queue=self._status_tracking_queue,
                errors_queue=self._errors_queue,
                family=metadata.run_id,
                last_queued_seq=self._last_queued_seq,
            ),
            sync_process.StatusTrackingThread(
                api_token=self._api_token,
                mode="async",
                project=metadata.project,
                errors_queue=self._errors_queue,
                status_tracking_queue=self._status_tracking_queue,
                last_ack_seq=self._last_ack_seq,
                last_ack_timestamp=self._last_ack_timestamp,
            ),
            ErrorsMonitor(errors_queue=self._errors_queue),
        ]

        for thread in self.threads:
            thread.start()

    def wait(
        self,
        verbose: bool = True,
    ) -> None:
        last_queued_sequence_id = self._sequence_tracker.last_sequence_id
        if last_queued_sequence_id == -1:
            return

        if verbose:
            logger.info("Waiting for all operations to be processed")

        sleep_time: float = 1.0
        last_print_timestamp: Optional[float] = None

        while True:
            try:
                with self._last_ack_timestamp:
                    self._last_ack_timestamp.wait(timeout=sleep_time)
                    value = self._last_ack_timestamp.value

                last_queued_sequence_id = self._sequence_tracker.last_sequence_id

                if value == -1:
                    last_print_timestamp = print_message(
                        "Waiting. No operations were processed yet. Operations to sync: %s",
                        self._sequence_tracker.last_sequence_id + 1,
                        last_print=last_print_timestamp,
                        verbose=verbose,
                    )
                elif value < last_queued_sequence_id:
                    last_print_timestamp = print_message(
                        "Waiting for remaining %d operation(s) to be processed",
                        last_queued_sequence_id - value + 1,
                        last_print=last_print_timestamp,
                        verbose=verbose,
                    )
                else:
                    # Reaching the last queued sequence ID means that all operations were submitted
                    if value >= last_queued_sequence_id:
                        break
            except KeyboardInterrupt:
                if verbose:
                    logger.warning("Waiting interrupted by user")
                return

        if verbose:
            logger.info("All operations were processed")

    def stop(self) -> None:
        for thread in self.threads:
            thread.interrupt()
        for thread in self.threads:
            thread.join()
        for thread in self.threads:
            if hasattr(thread, "close"):
                thread.close()
