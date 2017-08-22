#!/usr/bin/python
# coding=utf-8
import logging

import libmu.util
from libmu import tracker, TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState, IfElseState
from config import settings
from stages.util import default_trace_func


class FinalState(OnePassState):
    extra = "(sending quit)"
    expect = None
    command = "quit:"
    nextState = TerminalState

    def __init__(self, prevState):
        super(FinalState, self).__init__(prevState, trace_func=default_trace_func)


class ConfirmEmitState(OnePassState):
    extra = "(confirm emit)"
    expect = 'OK:EMIT'
    command = None
    nextState = FinalState

    def __init__(self, prevState):
        super(ConfirmEmitState, self).__init__(prevState, trace_func=default_trace_func)
        self.emit = prevState.emit
        self.out_key = prevState.out_key

    def post_transition(self):
        self.emit('frames', {'metadata': self.in_events['chunked_link']['metadata'], 'key': self.out_key})
        return self.nextState(self)  # don't forget this


class TryEmitState(OnePassState):
    extra = "(emit output)"
    expect = None
    command = 'emit:##TMPDIR##/out_0 {out_key}'
    nextState = ConfirmEmitState

    def __init__(self, prevState):
        super(TryEmitState, self).__init__(prevState, trace_func=default_trace_func)
        self.emit = prevState.emit
        self.out_key = prevState.out_key
        params = {'out_key': self.out_key}
        self.command = self.command.format(**params)


class CheckOutputState(IfElseState):
    extra = "(check output)"
    expect = 'OK:RETVAL('
    consequentState = TryEmitState
    alternativeState = FinalState

    def testfn(self):
        return self.messages[-1].startswith('OK:RETVAL(0)')

    def __init__(self, prevState):
        super(CheckOutputState, self).__init__(prevState, trace_func=default_trace_func)
        self.emit = prevState.emit
        self.out_key = prevState.out_key


class RunState(CommandListState):
    extra = "(run)"
    nextState = CheckOutputState
    commandlist = [(None, 'run:mkdir -p ##TMPDIR##/out_0/')
        , ('OK:RETVAL(0)', 'run:./youtube-dl -f "(mp4)" --get-url {URL} | head -n1 | xargs -IPLACEHOLDER '
                           './ffmpeg -y -ss {starttime} -t {duration} -i PLACEHOLDER -f image2 -c:v png '
                           '-start_number 1 ##TMPDIR##/out_0/%08d.png')
        , ('OK:RETVAL(0)', 'run:test `find ##TMPDIR##/out_0/ -name "*png" | wc -l` -gt 0')
                   # result will be used in next state
                   ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState, trace_func=default_trace_func)
        self.emit = prevState.emit
        self.out_key = prevState.out_key

        params = {'starttime': self.in_events['chunked_link']['starttime'],
                  'duration': self.in_events['chunked_link']['duration'],
                  'URL': self.in_events['chunked_link']['key'], 'out_key': self.out_key}
        logging.debug('params: ' + str(params))
        self.commands = [s.format(**params) if s is not None else None for s in self.commands]


class InitState(CommandListState):
    extra = "(init)"
    nextState = RunState
    commandlist = [("OK:HELLO", "seti:nonblock:0")
        , "run:rm -rf /tmp/*"
        , "run:mkdir -p ##TMPDIR##"
        , None
                   ]

    def __init__(self, prevState, in_events, emit):
        super(InitState, self).__init__(prevState, in_events=in_events, trace_func=default_trace_func)
        self.emit = emit
        self.out_key = settings['storage_base'] + in_events['chunked_link']['metadata'][
            'pipe_id'] + '/decode/' + libmu.util.rand_str(16) + '/'
        logging.debug('in_events: ' + str(in_events) + ', emit: ' + str(emit))
