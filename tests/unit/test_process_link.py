import multiprocessing
import os
import time
from multiprocessing import (
    Condition,
    Event,
    Process,
)
from typing import Optional
from unittest.mock import (
    Mock,
    call,
)

import psutil
import pytest
from pytest import fixture

from neptune_scale.util import ProcessLink


@fixture
def link() -> ProcessLink:
    return ProcessLink()


def start_link_worker(
    link: ProcessLink,
    *,
    sleep_before: Optional[float] = None,
    sleep_after: Optional[float] = None,
    should_start: bool = True,
    cond: Optional[Condition] = None,
    event: Optional[Event] = None,
):
    if sleep_before:
        time.sleep(sleep_before)

    if should_start:
        link.start()

    if sleep_after:
        time.sleep(sleep_after)

    if cond is not None:
        with cond:
            cond.notify_all()

    if event is not None:
        event.set()

    print("child exiting")


@pytest.mark.parametrize("sleep", [0, 0.001, 0.5, 1])
def test_successful_startup_and_termination(link, sleep):
    event = Event()

    p = Process(target=start_link_worker, args=(link,), kwargs=dict(sleep_after=sleep, event=event))
    t0 = time.monotonic()
    p.start()

    def callback(_):
        # Make sure we only get notified after the child process is dead
        assert time.monotonic() - t0 >= sleep
        assert event.is_set()

    link.start(on_link_closed=callback)


def test_join(link):
    """ProcessLink should only join after the other side terminates"""

    event = Event()

    p = Process(target=start_link_worker, args=(link,), kwargs=dict(event=event))
    p.start()

    mock = Mock()
    link.start(on_link_closed=mock)
    link.join()

    assert event.is_set()
    mock.assert_called()


def test_stop_does_not_call_on_link_closed(link):
    """After we call stop() we should never have the on_link_closed callback called."""

    p = Process(target=start_link_worker, args=(link,), kwargs=dict(sleep_after=1))
    p.start()

    mock = Mock()
    link.start(on_link_closed=mock)
    link.stop()

    mock.assert_not_called()


def test__start_timeout_no_start_on_child_end(link):
    """Child doesn't start the link at all"""

    p = Process(target=start_link_worker, args=(link,), kwargs=dict(should_start=False))
    p.start()

    assert not link.start(timeout=0.5)


def test__delayed_start_timeout(link):
    """Child starts the link but only after the allowed timeout."""

    p = Process(target=start_link_worker, args=(link,), kwargs=dict(sleep_before=1))
    p.start()

    assert not link.start(timeout=0.5)


def pong_worker(link, event):
    def on_message_received(parent_link, message):
        if message.startswith("ping"):
            parent_link.send(message.replace("ping", "pong"))
        else:
            parent_link.send("?")

    link.start(on_message_received=on_message_received)
    assert event.wait(1)


def test_message_passing(link):
    """Start a worker that responds to "ping" with "pong" and check if the message is passed correctly."""

    event = multiprocessing.Event()
    p = Process(target=pong_worker, args=(link, event))
    p.start()

    def on_msg(_, message):
        if message == "?":
            event.set()

    on_msg = Mock(side_effect=on_msg)

    link.start(on_message_received=on_msg)
    link.send("ping one")
    link.send("ping two")
    link.send("not-ping")

    assert event.wait(1)
    on_msg.assert_has_calls([call(link, "pong one"), call(link, "pong two"), call(link, "?")])


def parent(var, event):
    link = ProcessLink()
    p = multiprocessing.Process(target=child, args=(link, var, event))
    p.start()

    link.start()
    var.value = 1

    # Kill the parent process to simulate unexpected exit
    me = psutil.Process(os.getpid())
    me.kill()


def child(link, var, event):
    def on_closed(_):
        assert var.value == 1, "Parent exited early"
        event.set()
        raise SystemExit

    link.start(on_link_closed=on_closed)
    # We should never finish the sleep call, as on_closed raises SystemExit
    time.sleep(5)
    assert False, "on_closed callback was not called"


def test_parent_termination():
    """Child should detect parent termination"""

    # Used to signal child exit so we can finish the test
    event = multiprocessing.Event()
    # Set to 1 by parent process at exit
    var = multiprocessing.Value("i", 0)

    p = multiprocessing.Process(target=parent, args=(var, event))
    p.start()

    assert event.wait(1)
    assert var.value == 1
