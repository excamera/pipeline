#!/usr/bin/python
import Queue


class Pipeline(object):

    class Stage(object):
        def __init__(self, key, lambda_function, init_state, event, deliver_func=None, regions=None):
            self.key = key
            self.lambda_function = lambda_function
            self.init_state = init_state
            self.event = event
            self.deliver_func = deliver_func
            self.regions = regions
            self.downstreams = {}
            self.buffer_queue = Queue.Queue()
            self.deliver_queue = Queue.Queue()

        def __str__(self):
            pass

    def __init__(self):
        self.stages = {}

    def __str__(self):
        pass

    def add_stage(self, stage):
        if self.stages.has_key(stage.key):
            raise Exception('existing stage key')
        self.stages[stage.key] = stage

    def add_downstream(self, src_stage, downstream, key):
        if not isinstance(src_stage, Pipeline.Stage):
            src_stage = self.stages[src_stage]
        if isinstance(downstream, basestring):
            downstream = self.stages[downstream]
        if isinstance(downstream, Pipeline.Stage):
            downstream = downstream.buffer_queue
        src_stage.downstreams[key] = downstream
