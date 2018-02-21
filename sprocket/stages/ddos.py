#!/usr/bin/python
# coding=utf-8
import logging

from sprocket.util.misc import rand_str
from sprocket.controlling.tracker.machine_state import TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState, IfElseState
from sprocket.config import settings
from sprocket.stages import InitStateTemplate, FinalStateTemplate
from sprocket.stages.util import default_trace_func, get_output_from_message


class FinalState(FinalStateTemplate):
    pass

class RunState(CommandListState):
    nextState = FinalState
    commandlist = [ (None, "run:mkdir {outdir}"),
            ("OK:RETVAL(0)", "run:timeout -s INT {duration} ./goldeneye.py http://c09-30.sysnet.ucsd.edu/index.php/Main_Page -w {nworkers} -s {nsockets} -o {outdir}/log"), 
            ("OK:RETVAL(", "emit:{outdir} {out_key}"),
            ("OK:EMIT", "quit:")
                ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)
        key = self.in_events.keys()[0]
        out_key = settings['storage_base']+self.in_events['chunked_link']['metadata']['pipe_id']+'/goldeneye/'+rand_str(10)+'/'

        params = {'out_key': out_key, 'duration': self.config['duration'], 'nworkers': self.config['nworkers'], 'nsockets': self.config['nsockets'], 'outdir': self.config['outdir']}
        self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]
        self.emit_event(key, self.in_events.values()[0])  # just forward whatever comes in


class InitState(InitStateTemplate):
    nextState = RunState
