from dataclasses import dataclass


@dataclass(frozen=True)
class Operation:
    should_batch: bool
    step: int
    value: int


def is_batchable_fn(op: Operation) -> bool:
    return op.should_batch
