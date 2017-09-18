#!/usr/bin/python
import json
import logging
import pdb

import math

from libmu import tracker, TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState, IfElseState
from pipeline.config import settings
from pipeline.stages.util import default_trace_func, get_output_from_message, preprocess_config


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
        self.emit_event('person', {'metadata': metadata,
                                       'key': str(self.local['personId'])})
        return self.nextState(self)  # don't forget this


class GetOutputState(OnePassState):
    extra = "(check output)"
    expect = 'OK:RETVAL(0'
    command = None
    nextState = EmitState

    def __init__(self, prevState):
        super(GetOutputState, self).__init__(prevState)

    def post_transition(self):
        self.local['personId'] = get_output_from_message(self.messages[-1])
        return self.nextState(self)  # don't forget this


class RunState(CommandListState):
    extra = "(run)"
    nextState = GetOutputState
    commandlist = [(None, 'run: python googleFace_Mic_init_s3.py "{person}" 5 ##TMPDIR##')
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

    def __init__(self, prevState, in_events, emit_event,config):
        super(InitState, self).__init__(prevState, in_events=in_events, emit_event=emit_event,config=config, trace_func=default_trace_func)
        logging.debug('in_events: '+str(in_events))
