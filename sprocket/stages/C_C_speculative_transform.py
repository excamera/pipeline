#!/usr/bin/python
# coding=utf-8
import logging
import pdb

from sprocket.controlling.tracker.machine_state import TerminalState, CommandListState, OnePassState, IfElseState
from sprocket.stages import InitStateTemplate, util, FinalStateTemplate
from sprocket.stages.util import default_trace_func, get_output_from_message


class FinalState(FinalStateTemplate):
    pass


class WaitForAsyncEvents(OnePassState):
    expect = 'OK:'

    def __init__(self, prevState):
        super(WaitForAsyncEvents, self).__init__(prevState)

    def post_transition(self):
        emitdone = 'OK:EMIT('
        if self.messages[-1][:len(emitdone)] == emitdone:
            self.emit_event('chunks', {'metadata': self.in_events['chunks']['metadata'], 'key': self.local['out_key']})
            self.nextState = FinalState
        return self.nextState(self)  # don't forget this


WaitForAsyncEvents.nextState = WaitForAsyncEvents


# class RunSpeculativeState(CommandListState):
#     nextState = TryEmitState
#     commandlist = [(None, 'seti:nonblock:1'),
#                    ('', 'run:./ffmpeg -y -ss {own_starttime} -t 1 -i $(find ##TMPDIR##/in_0/ -name "*mp4") {transform} ##TMPDIR##/out_0/peer_output.mp4')
#                    ('')]
#
#     def __init__(self, prevState):
#         super(RunSpeculativeState, self).__init__(prevState)
#         params = {'key': self.in_events['chunks']['key'], 'out_key': self.local['out_key'], 'transform': self.config.get('transform', '-vf null'), 'own_starttime': 1-self.local['relative_pos']}
#         self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]


# class CheckPeerState(IfElseState):
#     consequentState = RunSpeculativeState
#     alternativeState = RunSpeculativeState
#
#     def testfn(self):
#         return True
#
#     def __init__(self, prevState):
#         super(CheckPeerState, self).__init__(prevState)


class RunState(CommandListState):
    extra = "(run)"
    nextState = WaitForAsyncEvents
    commandlist = [ (None, 'run:mkdir -p ##TMPDIR##/in_0/ ##TMPDIR##/out_0/')
                  , ('OK:RETVAL(0)', 'collect:{key} ##TMPDIR##/in_0')
                  , ('OK:COLLECT', 'run:./ffmpeg -y -ss 0 -t 1 -i $(find ##TMPDIR##/in_0/ -name "*mp4"|head -n1) {transform} ##TMPDIR##/out_0/own_output.mp4')
                  , ('OK:RETVAL(0)', 'seti:nonblock:1')
                  , ('OK:SETI(nonblock)', 'emit:##TMPDIR##/out_0/ {out_key}')
                  ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)
        self.local['out_key'] = util.get_output_key()
        params = {'key': self.in_events['chunks']['key'], 'out_key': self.local['out_key'], 'transform': self.config.get('transform', '-vf null'), 'own_starttime': self.local['relative_pos']}
        self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]


class InitState(InitStateTemplate):
    nextState = RunState

    def __init__(self, prevState, **kwargs):
        super(InitState, self).__init__(prevState, **kwargs)
        self.trace_func = lambda ev, msg, op: default_trace_func(ev, msg, op, stage='C_C_speculative_transform')
        self.local['relative_pos'] = (int(self.in_events['chunks']['metadata']['lineage'])-1) % 2 # 0 or 1 depending on preceding or succeeding its peer
