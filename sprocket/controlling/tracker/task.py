#!/usr/bin/python
from sprocket.stages import FinalStateTemplate


class Task(object):
    def __init__(self, lambda_func, init_state, event, regions=None, **kwargs):
        self.lambda_func = lambda_func
        self.constructor = init_state
        self.event = event
        self.regions = ["us-east-1"] if regions is None else regions
        self.kwargs = kwargs

        self.current_state = None
        self.rwflag = 0

    def __str__(self):
        return "task created" if self.current_state is None else self.current_state.__module__.split('.')[-1] + \
                                                                 ':' + self.current_state.__class__.__name__

    def rewire(self, ns):
        self.current_state = self.constructor(ns, task=self, **self.kwargs)

    def do_handle(self):
        self.current_state = self.current_state.do_handle()

    def do_read(self):
        self.current_state = self.current_state.do_read()

    def do_write(self):
        self.current_state = self.current_state.do_write()

    def send_async_msg(self, msg):
        self.current_state.outofband_msg(msg)

class TaskStarter(object):
    def __init__(self, ns):
        self.current_state = ns
        self.rwflag = 0

    def do_read(self):
        self.current_state.do_read()

    def do_write(self):
        self.current_state.do_write()

    def do_handle(self):
        raise Exception("TaskStarter can't handle any message, should have transitioned into a Task")


class OrphanedTask(Task):
    def __init__(self, *args, **kwargs):
        super(OrphanedTask, self).__init__(None, FinalStateTemplate, None)
