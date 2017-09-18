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
        metadata = self.in_events['video_link']['metadata']
        config = preprocess_config(self.config,
                                   {'fps': metadata['fps']})
        framesperchunk = (config.get('framesperchunk', metadata['fps']))  # default to 1 second chunk
        overlap = config.get('overlap', 0)

        i = 0
        while i * (framesperchunk - overlap) / metadata['fps'] < self.local['duration']:  # actual parallelizing here
            metacopy = metadata.copy()
            starttime = i * (framesperchunk - overlap) / metadata['fps']
            metacopy['lineage'] = str(i+1)
            endChunk = False
            if ((i+1)*(framesperchunk - overlap) / metadata['fps'])>= self.local['duration']:
                endChunk = True

            self.emit_event('chunked_link_forScene', {'metadata': metacopy,
                                       'key': self.in_events['video_link']['key'],
                                       'starttime': starttime,
                                       'frames': framesperchunk,
                                       'fps': metadata['fps'],
                                       'lineage':metacopy['lineage'],
                                       'end': endChunk})
            i += 1
        return self.nextState(self)  # don't forget this


class GetOutputState(OnePassState):
    extra = "(check output)"
    expect = 'OK:RETVAL(0'
    command = None
    nextState = EmitState

    def __init__(self, prevState):
        super(GetOutputState, self).__init__(prevState)

    def post_transition(self):
        output = json.loads(get_output_from_message(self.messages[-1]))
        self.local['duration'] = output['duration']
        self.in_events['video_link']['metadata']['fps'] = output['fps']
        return self.nextState(self)  # don't forget this


class RunState(CommandListState):
    extra = "(run)"
    nextState = GetOutputState
    commandlist = [(None, 'run:./youtube-dl -j {link} 2>/dev/null')
                   # output will be used in latter states
                   ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)

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

    def __init__(self, prevState, in_events, emit_event, config):
        super(InitState, self).__init__(prevState, in_events=in_events, emit_event=emit_event, config=config, trace_func=default_trace_func)
        logging.debug('in_events: '+str(in_events))
