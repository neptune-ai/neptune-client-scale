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
__all__ = ["NeptuneLoggingHandler"]

import logging
import threading
from datetime import (
    datetime,
    timedelta,
)
from typing import Optional

from neptune_scale import Run
from neptune_scale.api.validation import (
    verify_max_length,
    verify_type,
)
from neptune_scale.logging.console_log_capture import StepTracker
from neptune_scale.logging.logging_utils import (
    PartialLine,
    captured_data_to_lines,
    split_long_line,
)
from neptune_scale.sync.parameters import (
    MAX_ATTRIBUTE_PATH_LENGTH,
    MAX_STRING_SERIES_DATA_POINT_LENGTH,
)


class NeptuneLoggingHandler(logging.Handler):
    """
    A logging handler that sends the records created by the Python Logger to a Neptune run.

    Args:
        run: A reference to a Run object which should track the logs.
        level: Level of logs to capture. If not provided, defaults to `logging.NOTSET`.
        attribute_path: Path to the `StringSeries` attribute that stores the logs. If not provided,
            the logs are stored under "runtime/logs".

    Example:
        ```
        from neptune_scale import NeptuneLoggingHandler

        logger = logging.getLogger("my_experiment")

        npt_handler = NeptuneLoggingHandler(run=run, level=logging.INFO, attribute_path="messages/info")
        logger.addHandler(npt_handler)

        logger.info("Starting data preparation")
        ```
    """

    def __init__(
        self,
        *,
        run: Run,
        level: int = logging.NOTSET,
        attribute_path: Optional[str] = None,
    ) -> None:
        verify_type("run", run, Run)
        verify_type("level", level, int)
        verify_type("attribute_path", attribute_path, (str, type(None)))
        path = attribute_path if attribute_path else "runtime/logs"
        verify_max_length("attribute_path", path, MAX_ATTRIBUTE_PATH_LENGTH)

        super().__init__(level=level)
        self._path = path
        self._run = run
        self._step_tracker = StepTracker(run._fork_step if run._fork_step is not None else 0)
        self._thread_local = threading.local()

    def emit(self, record: logging.LogRecord) -> None:
        if not hasattr(self._thread_local, "inside_write"):
            self._thread_local.inside_write = False

        if not self._thread_local.inside_write:
            try:
                self._thread_local.inside_write = True
                formatted_record = self.format(record)
                self._log_record_to_run(formatted_record)
            finally:
                self._thread_local.inside_write = False

    def _log_record_to_run(self, formatted_record: str) -> None:
        current_ts = datetime.now()
        for ts, line in captured_data_to_lines(
            partial_line=PartialLine(),
            data=[(current_ts, formatted_record)],
            max_delay_before_flush=timedelta(seconds=0),  # we log synchronously, so no delay
        ):
            for short_line in split_long_line(line, MAX_STRING_SERIES_DATA_POINT_LENGTH):
                self._run.log_string_series(data={self._path: short_line}, step=self._step_tracker.value, timestamp=ts)
                self._step_tracker.increment()
