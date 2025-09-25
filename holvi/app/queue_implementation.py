from abc import ABC, abstractmethod
from queue import Queue
import os
from typing import Tuple

QUEUE_MAXSIZE = int(os.environ.get("QUEUE_SIZE", 1000))


class BasicQueueImplementation(ABC):
    @abstractmethod
    def get(self):
        pass

    @abstractmethod
    def put(self, item):
        pass

    @abstractmethod
    def join(self):
        pass

    @abstractmethod
    def task_done(self):
        pass


class InMemoryQueue(BasicQueueImplementation):
    def __init__(self, maxsize=QUEUE_MAXSIZE):
        self.queue = Queue(maxsize=maxsize)

    def get(self) -> Tuple:
        return self.queue.get()

    def put(self, item: Tuple):
        self.queue.put(item)

    def join(self):
        self.queue.join()

    def task_done(self):
        self.queue.task_done()
