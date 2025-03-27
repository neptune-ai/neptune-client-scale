from datetime import datetime
from typing import (
    Optional,
    Union,
)

from neptune_scale.api.metrics import Metrics
from neptune_scale.sync.metadata_splitter import MetadataSplitter
from neptune_scale.sync.operations_repository import OperationsRepository
from neptune_scale.sync.sequence_tracker import SequenceTracker

__all__ = ["AttributeStore"]


class AttributeStore:
    """
    Responsible for managing local attribute store, and pushing log() operations
    to the provided OperationsQueue -- assuming that there is something on the other
    end consuming the queue (which would be SyncProcess).
    """

    def __init__(
        self, project: str, run_id: str, operations_repo: OperationsRepository, sequence_tracker: SequenceTracker
    ) -> None:
        self._project = project
        self._run_id = run_id
        self._operations_repo = operations_repo
        self._sequence_tracker = sequence_tracker

    def log(
        self,
        timestamp: Optional[Union[datetime, float]] = None,
        configs: Optional[dict[str, Union[float, bool, int, str, datetime, list, set, tuple]]] = None,
        metrics: Optional[Metrics] = None,
        tags_add: Optional[dict[str, Union[list[str], set[str], tuple[str]]]] = None,
        tags_remove: Optional[dict[str, Union[list[str], set[str], tuple[str]]]] = None,
    ) -> None:
        if timestamp is None:
            timestamp = datetime.now()
        elif isinstance(timestamp, float):
            timestamp = datetime.fromtimestamp(timestamp)

        splitter: MetadataSplitter = MetadataSplitter(
            project=self._project,
            run_id=self._run_id,
            timestamp=timestamp,
            configs=configs,
            metrics=metrics,
            add_tags=tags_add,
            remove_tags=tags_remove,
        )

        operations = list(splitter)
        sequence_id = self._operations_repo.save_update_run_snapshots(operations)

        self._sequence_tracker.update_sequence_id(sequence_id)
