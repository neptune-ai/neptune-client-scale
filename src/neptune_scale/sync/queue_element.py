#
# Copyright (c) 2025, Neptune Labs Sp. z o.o.
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

__all__ = ("BatchedOperations", "SingleOperation")

from typing import (
    NamedTuple,
    Optional,
)


class BatchedOperations(NamedTuple):
    # Operation identifier of the last operation in the batch
    sequence_id: int
    # Timestamp of the last operation in the batch
    timestamp: float
    # Protobuf serialized (RunOperationBatch)
    operation: bytes


class SingleOperation(NamedTuple):
    # Operation identifier
    sequence_id: int
    # Timestamp of the operation being enqueued
    timestamp: float
    # Protobuf serialized (RunOperation)
    operation: bytes
    # Whether the operation is batchable. Eg. metadata updates are, while run creations are not.
    is_batchable: bool
    # Size of the metadata in the operation (without project, family, run_id etc.)
    metadata_size: Optional[int]
