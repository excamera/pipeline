#!/usr/bin/python
import json
import logging
import pdb

import math

from libmu import tracker, TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState, IfElseState
from config import settings
from stages.util import default_trace_func, get_output_from_message, preprocess_config


class FinalState(OnePassState):
    extra = "(sending quit)"
    expect = None
    command = "quit:"
    nextState = TerminalState

    def __init__(self, prevState):
        super(FinalState, self).__init__(prevState)


class EmitState(OnePassState):
    extra = "(emit)"
    expect = None
    command = None
    nextState = FinalState

    def __init__(self, prevState):
        super(EmitState, self).__init__(prevState)

    def post_transition(self):
        metadata = self.in_events['person']['metadata']
        #config = preprocess_config(metadata['configs']['parlink'],
        #                           {'fps': metadata['fps']})
        #framesperchunk = config.get('framesperchunk', metadata['fps'])  # default to 1 second chunk
        #overlap = config.get('overlap', 0)

        #i = 0
        #while i * (framesperchunk - overlap) / metadata['fps'] < self.local['duration']:
        #    metacopy = metadata.copy()
        #    starttime = i * (framesperchunk - overlap) / metadata['fps']
        #    metacopy['lineage'] = str(i+1)
        #self.emit_event('person', {'metadata': metadata,
                                        #'key': self.in_events['person']['key']})
        #                               'starttime': starttime,
        #                               'frames': framesperchunk})
        #    i += 1
        return self.nextState(self)  # don't forget this


class GetOutputState(OnePassState):
    extra = "(check output)"
    expect = 'OK:RETVAL(0'
    command = None
    nextState = EmitState

    def __init__(self, prevState):
        super(GetOutputState, self).__init__(prevState)

    def post_transition(self):
        #output = json.loads(get_output_from_message(self.messages[-1]))
        #self.local['duration'] = output['duration']
        #self.in_events['video_link']['metadata']['fps'] = output['fps']
        return self.nextState(self)  # don't forget this


class RunState(CommandListState):
    extra = "(run)"
    nextState = GetOutputState
    commandlist = [(None, 'run: python googleFace_init_s3.py "{person}" 5 ##TMPDIR##')
                   # output will be used in latter states
                   ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)

        params = {'person': self.in_events['person']['key']}
        self.commands = [s.format(**params) if s is not None else None for s in self.commands]


class InitState(CommandListState):
    extra = "(init)"
    nextState = RunState
    commandlist = [ ("OK:HELLO", "seti:nonblock:0")
                  , "run:rm -rf /tmp/*"
                  , "run:mkdir -p ##TMPDIR##"
                  , None
                  ]

    def __init__(self, prevState, in_events, emit_event):
        super(InitState, self).__init__(prevState, in_events=in_events, emit_event=emit_event, trace_func=default_trace_func)
        logging.debug('in_events: '+str(in_events))
