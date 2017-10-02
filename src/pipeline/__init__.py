#!/usr/bin/python
import Queue
import importlib
from time import strftime, localtime

from pipeline.config import settings
import libmu
import pipeline.stages
import pipeline.stages.util
from pipeline.util.durable_queue import DurableQueue
import pdb


class Pipeline(object):
    class Stage(object):
        def __init__(self, key, stage_name, lambda_function, init_state, config, event, deliver_func=None, regions=None):
            self.key = key
            self.stage_name = stage_name
            self.lambda_function = lambda_function
            self.event = event
            self.init_state = init_state
            self.config = config
            self.regions = regions
            self.deliver_func = deliver_func
            self.downstream_map = {}
            self.buffer_queues = {}
            self.deliver_queue = Queue.Queue()
            self.context = {}

        def __str__(self):
            return "Stage: %s, init_state: %s, downstreams: %s, deliver_func: %s" % (
                self.key, self.init_state, self.downstream_map.keys(), self.deliver_func)

        def emit(self, src_key, event):
            dstream = self.downstream_map[src_key]
            dstream[1].put({dstream[0]: event})

    def __init__(self, pipe_id=None):
        self.pipe_id = strftime("%Y%m%d%H%M%S", localtime()) + '-' + libmu.util.rand_str(
            4) if pipe_id is None else pipe_id
        self.stages = {}
        self.inputs = {}
        self.outputs = {}
        self.pipewide = {}

        self.scrub_bar_time = 0.0
        self.tasks = []

    def __str__(self):
        return "Pipeline: %s, stages: %s, inputs: %s, outputs: %s" % (
            self.pipe_id, self.stages.keys(), self.inputs.keys(), self.outputs.keys())

    def add_stage(self, stage):
        if stage.key in self.stages:
            raise Exception('existing stage key')
        self.stages[stage.key] = stage


    # def add_downstream(self, src_stage, downstream, key):
    #     if not isinstance(src_stage, Pipeline.Stage):
    #         src_stage = self.stages[src_stage]
    #     if isinstance(downstream, basestring):
    #         downstream = self.stages[downstream]
    #     if isinstance(downstream, Pipeline.Stage):
    #         downstream = downstream.buffer_queue
    #     src_stage.downstreams[key] = downstream


def create_from_spec(pipe_spec):
    pipe = Pipeline()

    for node in pipe_spec.get('nodes', []):
        importlib.import_module('pipeline.stages.' + node['stage'])
        init_state = eval('pipeline.stages.' + node['stage']).InitState
        event = node.get('event', pipeline.stages.util.get_default_event())
        event['lambda_function'] = node.get('lambda_function', settings['default_lambda_function'])
        pipe.add_stage(Pipeline.Stage(
            node['name']
            , node['stage']
            , event['lambda_function']
            , init_state
            , node.get('config', {})
            , node.get('event', event)
            , deliver_func=getattr(pipeline.stages.util, node.get('deliver_function', 'default_deliver_func'))
            , regions=None
        ))

    for stream in pipe_spec.get('streams', []):
        src_node = stream['src'].split(':')[0]
        src_key = stream['src'].split(':')[1]
        dst_node = stream['dst'].split(':')[0]
        dst_key = stream['dst'].split(':')[1]
        durable = stream.get('durable')

        if src_node is '' or src_key is '' or dst_node is '' or dst_key is '':
            raise Exception('stream format error: %s', stream)

        if src_node.startswith('input'):
            if dst_key not in pipe.stages[dst_node].buffer_queues:
                pipe.stages[dst_node].buffer_queues[dst_key] = DurableQueue() if durable is True else Queue.Queue()
            pipe.inputs[src_node] = (dst_key, pipe.stages[dst_node].buffer_queues[dst_key])  # each input src should only have one key
        elif dst_node.startswith('output'):
            if dst_node not in pipe.outputs:
                pipe.outputs[dst_node] = (dst_key, Queue.Queue())
            pipe.stages[src_node].downstream_map[src_key] = pipe.outputs[dst_node]
        else:
            if src_key in pipe.stages[src_node].downstream_map:
                raise Exception('existing src key: %s', stream)
            if dst_key not in pipe.stages[dst_node].buffer_queues:
                pipe.stages[dst_node].buffer_queues[dst_key] = DurableQueue() if durable is True else Queue.Queue()
            pipe.stages[src_node].downstream_map[src_key] = (dst_key, pipe.stages[dst_node].buffer_queues[dst_key])

    return pipe
