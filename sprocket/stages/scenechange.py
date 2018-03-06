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

        #extract just the pngs
        self.local['key_list'] = [str(self.in_events['frames']['key'] + \
                str(i).rjust(8,'0') + '.png') for i in range(1,self.in_events['frames']['nframes']+1)]

    def post_transition(self):

        metadata = self.in_events['frames']['metadata']
        fps = metadata['fps']
        self.local['lineage'] = self.in_events['frames']['metadata']['lineage']

        #put scene change markers
        t1 = self.local['start_time']
        timeMarkers = []
        for time in self.local['times']:
             timeMarkers.append(math.ceil((time-t1)*fps))
             t1 = time

        for i in xrange(len(self.local['key_list'])):

            sceneChange = False
            if i in timeMarkers:
                sceneChange = True

            #mark the last piece as a scenechange as well
            if metadata['end'] and (i == len(self.local['key_list'])-1):
                sceneChange = True

            self.emit_event('scene_list', {'metadata': self.in_events['frames']['metadata'],
                    'key': self.local['key_list'][i],'number':i+1,
                    'EOF': i == len(self.local['key_list'])-1, 'type': 'png',
                    'nframes': self.in_events['frames']['nframes'],
                    'me':self.in_events['frames']['metadata']['lineage'],
                    'switch': sceneChange
                    })
                    #TODO: Do i need to include seconds or startime?? i dont think so..

        return self.nextState(self)  # don't forget this


class GetOutputState(OnePassState):
    extra = "(check output)"
    expect = 'OK:RETVAL(0)'
    command = None
    nextState = EmitState

    def __init__(self, prevState):
        super(GetOutputState, self).__init__(prevState)

    def post_transition(self):
        output = (get_output_from_message(self.messages[-1])).splitlines()
        self.local['output'] = output

        self.local['times'] = []

        #find all the time stamps of scene changes
        for line in output[1:]:
            self.local['times'].append( float(self.local['start_time']) + float(line))

        return self.nextState(self)  # don't forget this


class RunState(CommandListState):
    extra = "(run)"
    nextState = GetOutputState
    #save all the times
    commandlist = [ (None, 'run:mkdir -p ##TMPDIR##/in_0')
                    ,('OK:RETVAL(0)', 'collect:{in_key} ##TMPDIR##/in_0')
                    , ('OK:COLLECT', 'run: ./ffmpeg -start_number 1 -i ##TMPDIR##/in_0/%08d.png \
                    -vf "select=gt(scene\,0.1),showinfo" -f null - > ##TMPDIR##/out1.txt 2>&1')
                    , ('run: cat ##TMPDIR##/out1.txt')
                    , ("run:grep -Poe '(?<=pts_time:).*(?=pos)|(?<=Duration: ).*(?=, start)'\
                    ##TMPDIR##/out1.txt")
                   ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)
        self.local['start_time'] = self.in_events['frames']['seconds'][0]
        params = {'in_key':self.in_events['frames']['key']}
        logging.debug('params: ' + str(params))
        self.commands = [s.format(**params) if s is not None else None for s in self.commands]


class InitState(InitStateTemplate):
    nextState = RunState

    def __init__(self, prevState, **kwargs):
        super(InitState, self).__init__(prevState, **kwargs)
        self.trace_func = lambda ev, msg, op: default_trace_func(ev, msg, op, stage='scenechange')
