import multiprocessing as mp
from multiprocessing import Process
from multiprocessing import Event as pEvent
from queue import Empty, Full
from time import sleep
from threading import Thread
from threading import Event as tEvent
from typing import Any, Callable, Iterable, Mapping
import warnings

from seqqueue import SeqQueue

__author__ = "Adrian Krueger"
__copyright__ = "Adrian Krueger"
__license__ = "MIT"


class ThreadWorker(Thread):
    def __init__(
        self,
        target: Callable[..., object],
        in_q: SeqQueue,
        out_q: SeqQueue | None,
        stop_event: tEvent,
        error_event: tEvent,
        group: None = None,
        name: str | None = None,
        args: Iterable[Any] = (),
        kwargs: Mapping[str, Any] | None = None,
        *,
        daemon: bool | None = None,
    ) -> None:
        super().__init__(group, target, name, args, kwargs, daemon=daemon)
        self.in_q = in_q
        self.out_q = out_q
        self.stop_event = stop_event
        self.error_event = error_event

    def run(self):
        try:
            while True:
                # Get item from in_q
                while not self.stop_event.is_set():
                    try:
                        index, item = self.in_q.get(block=True, timeout=0.1)
                        break
                    except Empty:
                        continue
                else:
                    return

                # Apply target function
                index, item = index, self._target(item, *self._args, **self._kwargs)

                # Put item to out_q
                if self.out_q is not None:
                    while not self.stop_event.is_set():
                        try:
                            self.out_q.put((index, item), block=True, timeout=0.1)
                            break
                        except Full:
                            continue
                    else:
                        return

                self.in_q.task_done()

        except Exception as e:
            try:
                self.error_event.set()
                # Notify other to stop
                self.stop_event.set()
            finally:
                raise (e)
        finally:
            # Avoid a refcycle if the thread is running a function with
            # an argument that has a member that points to the thread.
            del self._target, self._args, self._kwargs


class ProcessWorker(Process):
    def __init__(
        self,
        target: Callable[..., object],
        in_q: SeqQueue,
        out_q: SeqQueue | None,
        stop_event: pEvent,
        error_event: pEvent,
        group: None = None,
        name: str | None = None,
        args: Iterable[Any] = (),
        kwargs: Mapping[str, Any] = {},
        *,
        daemon: bool | None = None,
    ) -> None:
        super().__init__(group, target, name, args, kwargs, daemon=daemon)
        self.in_q = in_q
        self.out_q = out_q
        self.stop_event = stop_event
        self.error_event = error_event

    def run(self):
        try:
            while True:
                # Get item from in_q
                while not self.stop_event.is_set():
                    try:
                        index, item = self.in_q.get(block=True, timeout=0.1)
                        break
                    except Empty:
                        continue
                else:
                    return

                # Apply target function
                index, item = index, self._target(item, *self._args, **self._kwargs)

                # Put item to out_q
                if self.out_q is not None:
                    while not self.stop_event.is_set():
                        try:
                            self.out_q.put((index, item), block=True, timeout=0.1)
                            break
                        except Full:
                            continue
                    else:
                        return

                self.in_q.task_done()

        except Exception as e:
            try:
                self.error_event.set()
                # Notify others to stop
                self.stop_event.set()
            finally:
                raise (e)


class StreamPipe:
    """StreamPipe enables sequential, multi-worker manipulation of incoming data.

    >>> # Print 0,...,99
    >>> pipe = StreamPipe()
    >>> pipe.add(lambda x: x**0.5, n_workers=5, maxsize = 5)
    >>> pipe.add(lambda x: print(x), n_workers=1, maxsize = 5)
    >>> with pipe:
    >>>     for i in range(100):
    >>>         pipe.put(i**2)

    """

    def __init__(self) -> None:
        self.index = 0
        self._worker_groups: Iterable[Iterable[ThreadWorker | ProcessWorker]] = []
        self._stop_event = tEvent()
        self._error_event = tEvent()
        self._manager = None

    def put(self, item: Any, block: bool = True, timeout: float | None = None) -> None:
        """Put an item on the StreamPipe.

        Works just like queue.put(). Empty and Full exceptions/blocks are raised with
        respect to the 'maxsize' of the first added worker group.

        Args:
            item (Any): The item to put on the StreamPipe.
            block (bool, optional): Block until a slot becomes available at most 'timeout'
                seconds. Defaults to True.
            timeout (float | None, optional): Maximum block timeout in seconds.
                Defaults to None.

        """
        if not self._worker_groups:
            # No worker has been added, do nothing
            return
        self._worker_groups[0][0].in_q.put((self.index, item), block, timeout)
        self.index += 1

    def add(
        self,
        target: Callable,
        n_workers: int,
        maxsize: int = 0,
        multiprocessing: bool = False,
        args: Iterable[Any] = (),
        kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        """Add a worker group to the StreamPipe.

        A worker group consists of 'n_workers' concurrent workers running 'target'
        on the next incoming item. The item must be the first argument of 'target'.
        If subsequent worker groups are added, 'target' must return the next item.

        >>> # Add a worker group of 5 returning the square of x.
        >>> pipe.add(lambda x: x**2, n_workers=5, maxsize = 3)
        >>> # For multiprocessing workers
        >>> pipe.add(lambda x: print(x), n_workers=1, maxsize = 3, multiprocessing+True)

        Args:
            target (Callable): The target function of the workers.
            n_workers (int): The number of concurrent workers executing 'target' on
                incoming data.
            maxsize (int, optional): The maxsize of the incoming data queue.
                Defaults to 0.
            multiprocessing (bool, optional): If True, workers run in seperate
                processes (rather than threads). Defaults to False.
            args (Iterable[Any], optional): Addtional Args passed to 'target'.
                Defaults to ().
            kwargs (Mapping[str, Any] | None, optional): Kwargs passed to 'target'.
                Defaults to None.
        """

        if n_workers < 1:
            return

        # Create worker group
        if multiprocessing:
            self._init_mp()
            in_q = self._manager.SeqQueue(maxsize=maxsize, start_index=self.index)
            worker_group = [
                ProcessWorker(
                    target=target,
                    in_q=in_q,
                    out_q=None,
                    stop_event=self._stop_event,
                    error_event=self._error_event,
                    args=args,
                    kwargs=kwargs if kwargs is not None else {},
                    name=f"{target.__name__}_worker{len(self._worker_groups)}_{i}",
                    daemon=True,
                )
                for i in range(n_workers)
            ]
        else:
            if len(self._worker_groups) > 0 and not isinstance(
                self._worker_groups[-1][0].in_q, SeqQueue
            ):
                # The preceding worker group has multiprocessing
                in_q = self._manager.SeqQueue(maxsize=maxsize, start_index=self.index)
            else:
                in_q = SeqQueue(maxsize=maxsize, start_index=self.index)
            worker_group = [
                ThreadWorker(
                    target=target,
                    in_q=in_q,
                    out_q=None,
                    stop_event=self._stop_event,
                    error_event=self._error_event,
                    args=args,
                    kwargs=kwargs,
                    name=f"{target.__name__}_worker{len(self._worker_groups)}_{i}",
                    daemon=True,
                )
                for i in range(n_workers)
            ]

        # Attach to previous worker group (if any)
        if len(self._worker_groups) > 0:
            for worker in self._worker_groups[-1]:
                worker.out_q = in_q

        # Add to worker groups
        self._worker_groups.append(worker_group)

    def start(self):
        """Start all workers.

        Raises:
            ValueError: If StreamPipe has been stopped.
        """
        if self._stop_event.is_set():
            raise RuntimeError("StreamPipe cannot be restarted")

        for worker_group in self._worker_groups:
            for worker in worker_group:
                worker.start()

    def stop(self):
        """Stop all workers."""
        self._stop_event.set()

    def join_queues(self):
        """Block until all current tasks on the StreamPipe are finished."""
        for worker_group in self._worker_groups:
            worker_group[0].in_q.join()

    def join_workers(self):
        """Block until all workers are stopped."""
        for worker_group in self._worker_groups:
            for worker in worker_group:
                worker.join()

    def unfinished_tasks(self) -> int:
        """Return the total number of unfinished tasks on the StreamPipe.

        Returns:
            int: Total number of unfinished tasks
        """
        unfinished_tasks = 0
        for worker_group in self._worker_groups:
            unfinished_tasks += worker_group[0].in_q.unfinished_tasks
        return unfinished_tasks

    def _init_mp(self):
        if self._manager is None:
            self._manager = mp.Manager()
            self._stop_event = self._manager.Event()
            self._error_event = self._manager.Event()
            for worker_group in self._worker_groups:
                for worker in worker_group:
                    worker.stop_event = self._stop_event
                    worker.error_event = self._error_event

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        # Wait until all unfinished tasks are completed
        while self.unfinished_tasks() > 0:
            if self._stop_event.is_set():
                self.join_workers()
                if self._error_event.is_set():
                    warnings.warn(
                        f"StreamPipe stopped due to a worker error. A total of {self.unfinished_tasks()} tasks could not be completed",
                        RuntimeWarning,
                    )
                else:
                    warnings.warn(
                        f"StreamPipe has been stopped, but a total of {self.unfinished_tasks()} tasks could not be completed",
                        RuntimeWarning,
                    )
                return
            sleep(0.01)
        # Stop StreamPipe and wait until all workers are shutdown.
        self.stop()
        self.join_workers()
