from __future__ import annotations

__all__ = ("ErrorsQueue",)

from multiprocessing import Queue

from neptune_scale.core.components.abstract import Resource


class ErrorsQueue(Resource):
    def __init__(self) -> None:
        self._errors_queue: Queue[BaseException] = Queue()

    def put(self, error: BaseException) -> None:
        self._errors_queue.put(error)

    def get(self, block: bool = True, timeout: float | None = None) -> BaseException:
        return self._errors_queue.get(block=block, timeout=timeout)

    def cleanup(self) -> None:
        pass

    def close(self) -> None:
        self._errors_queue.close()
