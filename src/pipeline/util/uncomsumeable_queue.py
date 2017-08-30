#!/usr/bin/python
import copy
import Queue
import threading


class UnconsumeableQueue(object):

    def __init__(self):
        self._set = False
        self._element = None
        self._lock = threading.Lock()

    def put(self, e):
        with self._lock:
            if self._set:
                raise Queue.Full()
            self._element = e
            self._set = True

    def get(self):
        if not self._set:
            raise Queue.Empty()
        return copy.deepcopy(self._element)

    def empty(self):
        return not self._set

    def full(self):
        return self._set
