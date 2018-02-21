#!/usr/bin/python
import copy
import Queue
import threading
from collections import deque


class DurableQueue(object):

    def __init__(self):
        self.queue = deque()
        self._lock = threading.Lock()

    def put(self, e):
        with self._lock:
            if len(self.queue) > 0:
                raise Queue.Full()
            self.queue.append(e)

    def get(self):
        with self._lock:
            if len(self.queue) == 0:
                raise Queue.Empty()
            return copy.deepcopy(self.queue[0])

    def empty(self):
        return len(self.queue) == 0

    def full(self):
        return len(self.queue) > 0

    def clear(self):
        with self._lock:
            self.queue.clear()
