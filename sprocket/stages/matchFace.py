#!/usr/bin/python
import json
import logging
import pdb


from sprocket.controlling.tracker.machine_state import TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState, IfElseState
from sprocket.config import settings
from sprocket.stages import InitStateTemplate
from sprocket.stages.util import default_trace_func, get_output_from_message, preprocess_config,staged_trace_func



class FinalState(OnePassState):
    extra = "(sending quit)"
    expect = None
    command = "quit:"
    nextState = TerminalState

    def __init__(self, prevState):
        super(FinalState, self).__init__(prevState)
        #write to the pipe dictionary how many frames we expect to be for encode
        self.pipe['person'] = str(self.in_events['person']['key']+ '.jpg')

class GetOutputState(OnePassState):
    extra = "(check output)"
    expect = 'OK:RETVAL(0'
    command = None
    nextState = FinalState

    def __init__(self, prevState):
        super(GetOutputState, self).__init__(prevState)

    def post_transition(self):
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
    nextState = RunState
    extra = "(init)"
    commandlist = [ ("OK:HELLO", "seti:nonblock:0")
                  # , "run:rm -rf /tmp/*"
                  , "run:mkdir -p ##TMPDIR##"
                  , None
                  ]    
    
    def __init__(self, prevState, **kwargs):
        super(InitState, self).__init__(prevState, **kwargs)
        self.trace_func = lambda ev, msg, op: default_trace_func(ev, msg, op, stage='parallelize_link')

