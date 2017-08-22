#!/usr/bin/python
import json
import logging
import pdb

import math

from libmu import tracker, TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState, IfElseState
from config import settings
from stages.util import default_trace_func, get_output_from_message


class FinalState(OnePassState):
    extra = "(sending quit)"
    expect = None
    command = "quit:"
    nextState = TerminalState

    def __init__(self, prevState):
        super(FinalState, self).__init__(prevState, trace_func=default_trace_func)


class EmitState(OnePassState):
    extra = "(emit)"
    expect = None
    command = None
    nextState = FinalState

    def __init__(self, prevState):
        super(EmitState, self).__init__(prevState, trace_func=default_trace_func)
        self.emit = prevState.emit
        self.duration = prevState.duration

    def post_transition(self):
        for i in xrange(int(math.ceil(self.duration))):
            metadata = self.in_events['video_link']['metadata'].copy()
            metadata['lineage'] = str(i+1)
            self.emit('chunked_link', {'metadata': metadata,
                                       'key': self.in_events['video_link']['key'],
                                       'starttime': i,
                                       'duration': 1})
        return self.nextState(self)  # don't forget this


class GetOutputState(OnePassState):
    extra = "(check output)"
    expect = 'OK:RETVAL(0'
    command = None
    nextState = EmitState

    def __init__(self, prevState):
        super(GetOutputState, self).__init__(prevState, trace_func=default_trace_func)
        self.emit = prevState.emit
        self.duration = None

    def post_transition(self):
        output = json.loads(get_output_from_message(self.messages[-1]))
        self.duration = output['duration']
        self.in_events['video_link']['metadata']['fps'] = output['fps']
        return self.nextState(self)  # don't forget this


class RunState(CommandListState):
    extra = "(run)"
    nextState = GetOutputState
    commandlist = [(None, 'run:./youtube-dl -j {link}')
                   # output will be used in latter states
                   ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState, trace_func=default_trace_func)
        self.emit = prevState.emit

        params = {'link': self.in_events['video_link']['key']}
        self.commands = [s.format(**params) if s is not None else None for s in self.commands]


class InitState(CommandListState):
    extra = "(init)"
    nextState = RunState
    commandlist = [ ("OK:HELLO", "seti:nonblock:0")
                  , "run:rm -rf /tmp/*"
                  , "run:mkdir -p ##TMPDIR##"
                  , None
                  ]

    def __init__(self, prevState, in_events, emit):
        super(InitState, self).__init__(prevState, in_events=in_events, trace_func=default_trace_func)
        self.emit = emit
        logging.debug('in_events: '+str(in_events)+', emit: '+str(emit))
