#!/usr/bin/python
# coding=utf-8
import logging
import json
import pdb
import math
import time 
from sprocket.util.misc import rand_str
from sprocket.controlling.tracker.machine_state import TerminalState, CommandListState, OnePassState, IfElseState
from sprocket.config import settings
from sprocket.stages import InitStateTemplate, CreateTarStateTemplate,ExtractTarStateTemplate, FinalStateTemplate
from sprocket.stages.util import default_trace_func, get_output_from_message
from copy import deepcopy

class FinalState(FinalStateTemplate):
    pass


class EmitState(OnePassState):
    extra = "(emit)"
    expect = None
    command = None
    nextState = FinalState

    def __init__(self, prevState):
        super(EmitState, self).__init__(prevState)

    def post_transition(self):


        for i in xrange(len(self.local['key_list'])):

            metadata = self.in_events['scene_list']['metadata']
            self.local['lineage'] = metadata['lineage']
            metadata['rek'] = self.local['rek']#whether rek was successful
            metadata['me'] = metadata['lineage']#for benchmarking
            metadata['boundingbox'] = self.local['output']


            self.emit_event('frame', {'metadata': metadata,
                    'key': self.local['key_list'][i],'number':i+1,
                    'EOF': i == len(self.local['key_list'])-1,
                    'type':self.in_events['scene_list']['type'],
                    'fps': metadata['fps'],
                    'lineage':metadata['lineage'],
                    'nframes':self.in_events['scene_list']['metadata']['duration'],
                    'switch': True
                    })


        return self.nextState(self)  # don't forget this


class GetOutputState(OnePassState):
    extra = "(check output)"
    expect = "OK:RETVAL(0)"
    command = None
    nextState = EmitState

    def __init__(self, prevState):
        super(GetOutputState, self).__init__(prevState)

    def post_transition(self):

        matchBox = get_output_from_message(self.messages[-1])

        self.local['output'] = matchBox

        if str(matchBox) == "False\n":
            self.local['rek'] = False
        else:
            self.local['rek'] = True

        return self.nextState(self)  # don't forget this
class CreateTarState(CreateTarStateTemplate):
    nextState =GetOutputState
    tar_dir = '##TMPDIR##/out_0'


class RunState(CommandListState):
    extra = "(run)"
    nextState = CreateTarState if settings.get('use_tar') else GetOutputState
    commandlist = [ (None, 'run:mkdir -p ##TMPDIR##/in_0/')
                  , ('OK:RETVAL(0)', 'run: python rek.py ' +\
                          '"{person}" {key_list} {bucket} 300 70 10 0.1 > ##TMPDIR##/in_0/temp.txt')
                  , ('OK:RETVAL(0)', 'run:cat ##TMPDIR##/in_0/temp.txt')
                    #get output in next stage
                    ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)

        self.local['bucket'] = settings['storage_base'].split('s3://')[1].split('/')[0]
        key_list = []
        for i in xrange(len(self.in_events['scene_list']['key_list'])):
            key_list.append(self.in_events['scene_list']['key_list'][i])


        self.local['key_list'] = key_list

        #stripping bucket to make keylist compatible with stage
        key_list = [k.split(self.local['bucket'])[1][1:] for k in key_list]

        params = {'key_list': ' '.join(key_list),
                'person':self.pipe['person'],
                'bucket': self.local['bucket']}
        logging.debug('params: '+str(params))
        self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]


class InitState(InitStateTemplate):
    nextState = RunState

    def __init__(self, prevState, **kwargs):
        super(InitState, self).__init__(prevState, **kwargs)
        self.trace_func = lambda ev, msg, op: default_trace_func(ev, msg, op, stage='rek')
        lineage = int(self.in_events['scene_list']['metadata']['lineage'])
        nframes = len(self.in_events['scene_list']['key_list'])
