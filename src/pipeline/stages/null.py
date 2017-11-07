#!/usr/bin/python
# coding=utf-8
import logging

import libmu.util
from libmu import tracker, TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState, IfElseState
from pipeline.config import settings
from pipeline.stages import InitStateTemplate, FinalStateTemplate
from pipeline.stages.util import default_trace_func, get_output_from_message


class FinalState(FinalStateTemplate):
    pass


class RunState(OnePassState):
    expect = None
    command = None
    nextState = FinalState

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)
        key = self.in_events.keys()[0]
        self.emit_event(key, self.in_events.values()[0])  # just forward whatever comes in


class InitState(InitStateTemplate):
    nextState = RunState
