#!/usr/bin/python
# coding=utf-8

from sprocket.controlling.tracker.machine_state import CommandListState, OnePassState
from sprocket.stages import InitStateTemplate, FinalStateTemplate


class FinalState(FinalStateTemplate):
    pass

class EmitState(OnePassState):
    nextState = FinalState

    def __init__(self, prevState):
        super(EmitState, self).__init__(prevState)
        key = self.in_events.keys()[0]
        self.emit_event(key, self.in_events.values()[0])  # just forward whatever comes in
        

class RunState(CommandListState):
    nextState = EmitState
    commandlist = [ (None, "run:{cmd}"), 
            ("OK:", "quit:")
                ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)

        params = {'cmd': self.config['cmd']}
        self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]


class InitState(InitStateTemplate):
    nextState = RunState
