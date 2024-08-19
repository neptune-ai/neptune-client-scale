__all__ = ["kill_me"]

import os
from typing import List

import psutil

from neptune_scale.envs import SUBPROCESS_KILL_TIMEOUT

KILL_TIMEOUT = int(os.getenv(SUBPROCESS_KILL_TIMEOUT, "5"))


def kill_me() -> None:
    process = psutil.Process(os.getpid())
    try:
        children = _get_process_children(process)
    except psutil.NoSuchProcess:
        children = []

    for child_proc in children:
        _terminate(child_proc)
    _, alive = psutil.wait_procs(children, timeout=KILL_TIMEOUT)
    for child_proc in alive:
        _kill(child_proc)
    # finish with terminating self
    _terminate(process)


def _terminate(process: psutil.Process) -> None:
    try:
        process.terminate()
    except psutil.NoSuchProcess:
        pass


def _kill(process: psutil.Process) -> None:
    try:
        if process.is_running():
            process.kill()
    except psutil.NoSuchProcess:
        pass


def _get_process_children(process: psutil.Process) -> List[psutil.Process]:
    try:
        return process.children(recursive=True)
    except psutil.NoSuchProcess:
        return []
