import threading
from time import sleep
from timeit import default_timer

import pytest

from streampipe import StreamPipe


__author__ = "Adrian Krueger"
__copyright__ = "Adrian Krueger"
__license__ = "MIT"


def _identity(x):
    return x


def _square(x):
    return x**2


def _root(x):
    return x**0.5


def _append(x, l):
    l.append(x)
    return x


def _error_on_2(x):
    if x == 2:
        raise Exception()
    return x


def test_sample_run():
    target = [float(i) for i in range(100)]
    test = []

    streampipe = StreamPipe()
    streampipe.add(_square, n_workers=5, maxsize=3)
    streampipe.add(_identity, n_workers=5, maxsize=3)
    streampipe.add(_root, n_workers=5, maxsize=3)
    streampipe.add(_append, n_workers=1, maxsize=2, args=(test,))

    streampipe.start()
    for i in range(100):
        streampipe.put(i)
    streampipe.join_queues()
    streampipe.stop()
    streampipe.join_workers()

    assert target == test


def test_order():
    target = [i for i in range(100)]
    test = []

    streampipe = StreamPipe()
    streampipe.add(_identity, n_workers=10, maxsize=3)
    streampipe.add(_identity, n_workers=10, maxsize=3)
    streampipe.add(_identity, n_workers=10, maxsize=3)
    streampipe.add(_identity, n_workers=10, maxsize=3)
    streampipe.add(_identity, n_workers=10, maxsize=3)
    streampipe.add(_identity, n_workers=10, maxsize=3)
    streampipe.add(_append, n_workers=1, maxsize=2, args=(test,))

    with streampipe:
        for i in range(100):
            streampipe.put(i)

    assert test == target


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_target_error():
    streampipe = StreamPipe()
    streampipe.add(_identity, n_workers=2, maxsize=2)
    streampipe.add(_error_on_2, n_workers=2, maxsize=2)

    with streampipe:
        streampipe.put(1)  # ok
        streampipe.put(2)  # worker fails

    assert len(list(threading.enumerate())) == 1
    streampipe.join_workers()  # should be finished
    with pytest.raises(Exception):
        streampipe.start()


def test_delete():
    streampipe = StreamPipe()
    streampipe.add(_identity, n_workers=2, maxsize=2)
    streampipe.start()

    start = default_timer()
    while len(list(threading.enumerate())) != 3:
        sleep(0.01)
        assert default_timer() - start < 1

    streampipe.stop()

    start = default_timer()
    while len(list(threading.enumerate())) != 1:
        sleep(0.01)
        assert default_timer() - start < 1
