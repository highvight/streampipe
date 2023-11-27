import pytest

import numpy as np
from numpy.random import rand

from streampipe import StreamPipe


__author__ = "Adrian Krueger"
__copyright__ = "Adrian Krueger"
__license__ = "MIT"


ROUNDS = 5
N_IMAGES = 100


def _reshape(img):
    return img[::2, ::2]


def _standardize(img):
    mean = np.mean(img, axis=(0, 1), keepdims=True)
    std = np.std(img, axis=(0, 1), keepdims=True)
    return (img - mean) / std


def _test_baseline(img: np.array):
    for _ in range(N_IMAGES):
        x = _reshape(img)
        x = _standardize(x)


def _test_pipe(pipe: StreamPipe, img: np.array):
    for _ in range(N_IMAGES):
        pipe.put(img)
    pipe.join_queues()
    pipe.stop()


@pytest.mark.parametrize("n_workers", [""])
@pytest.mark.parametrize("maxsize", [""])
def test_baseline(benchmark, n_workers, maxsize):
    img = rand(1000, 1000, 3) * 255
    benchmark.pedantic(_test_baseline, args=(img,), rounds=ROUNDS)


@pytest.mark.parametrize("n_workers", [1, 4, 8])
@pytest.mark.parametrize("maxsize", [1, 4, 8])
def test_pipe_multithread(benchmark, maxsize, n_workers):
    def setup():
        img = rand(1000, 1000, 3) * 255
        pipe = StreamPipe()
        pipe.add(_reshape, n_workers=n_workers, maxsize=maxsize)
        pipe.add(_standardize, n_workers=n_workers, maxsize=maxsize)
        pipe.start()
        return (pipe, img), {}

    benchmark.pedantic(_test_pipe, setup=setup, rounds=ROUNDS)


# Verz slow multiprocessing performance

# @pytest.mark.parametrize("n_workers", [1, 4, 8])
# @pytest.mark.parametrize("maxsize", [1,4,8])
# def test_pipe_multiprocess(benchmark, maxsize, n_workers):
#     def setup():
#         img = rand(1000, 1000, 3) * 255
#         pipe = StreamPipe()
#         pipe.add(_reshape, n_workers=n_workers, maxsize=maxsize, multiprocessing=True)
#         pipe.add(_standardize, n_workers=n_workers, maxsize=maxsize, multiprocessing=True)
#         pipe.start()
#         return (pipe, img), {}

#     benchmark.pedantic(_test_pipe, setup=setup, rounds=ROUNDS)
