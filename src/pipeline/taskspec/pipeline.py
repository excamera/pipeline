#!/usr/bin/python
import Queue
import importlib
import json
from time import strftime, localtime
from config import settings
import libmu
import stages
import stages.util
import pdb


class Pipeline(object):
    class Stage(object):
        def __init__(self, key, lambda_function, init_state, conf, event, deliver_func=None, regions=None):
            self.key = key
            self.lambda_function = lambda_function
            self.event = event
            self.init_state = init_state
            self.conf = conf
            self.regions = regions
            self.deliver_func = deliver_func
            self.downstreams = {}
            self.buffer_queue = Queue.Queue()
            self.deliver_queue = Queue.Queue()

        def __str__(self):
            return "Stage: %s, init_state: %s, downstreams: %s, deliver_func: %s" % (
                self.key, self.init_state, self.downstreams.keys(), self.deliver_func)

        def emit(self, src_key, event):
            self.downstreams[src_key]['dst_node'].put({self.downstreams[src_key]['dst_key']: event})

    def __init__(self, pipe_id=None):
        self.pipe_id = strftime("%Y%m%d%H%M%S", localtime()) + '-' + libmu.util.rand_str(
            4) if pipe_id is None else pipe_id
        self.stages = {}
        self.inputs = {}
        self.outputs = {}

    def __str__(self):
        return "Pipeline: %s, stages: %s, inputs: %s, outputs: %s" % (
            self.pipe_id, self.stages.keys(), self.inputs.keys(), self.outputs.keys())

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


def create_from_spec(pipe_spec):
    pipe = Pipeline()

    for node in pipe_spec.get('nodes', []):
        importlib.import_module('stages.' + node['stage'])
        init_state = eval('stages.' + node['stage']).InitState
        pipe.add_stage(Pipeline.Stage(
            node['name']
            , node.get('lambda_function', settings['default_lambda_function'])
            , init_state
            , node.get('config', {})
            , node.get('event', stages.util.get_default_event())
            , deliver_func=getattr(stages.util, node.get('deliver_function', 'default_deliver_func'))
            , regions=None
        ))

    for stream in pipe_spec.get('streams', []):
        src_node = stream['src'].split(':')[0]
        src_key = stream['src'].split(':')[1]
        dst_node = stream['dst'].split(':')[0]
        dst_key = stream['dst'].split(':')[1]

        if src_node is '' or src_key is '' or dst_node is '' or dst_key is '':
            raise Exception('stream format error: %s', stream)

        if src_node.startswith('input'):
            pipe.inputs[src_node] = {'dst_node': pipe.stages[dst_node].buffer_queue, 'dst_key': dst_key}
        elif dst_node.startswith('output'):
            if not pipe.outputs.has_key(dst_node):
                pipe.outputs[dst_node] = {'dst_node': Queue.Queue(), 'dst_key': dst_key}
            pipe.stages[src_node].downstreams[src_key] = pipe.outputs[dst_node]
        else:
            if pipe.stages[src_node].downstreams.has_key(src_key):
                raise Exception('existing src key: %s', stream)
            pipe.stages[src_node].downstreams[src_key] = {'dst_node': pipe.stages[dst_node].buffer_queue,
                                                          'dst_key': dst_key}

    return pipe
