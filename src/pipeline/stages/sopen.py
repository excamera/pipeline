"""
trying TCP hole punching in Lambda by simultaneous open, not working yet.
"""
#!/usr/bin/python
# coding=utf-8
import logging
import time
import libmu.util
from libmu import tracker, TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState, IfElseState
from pipeline.config import settings
from pipeline.stages import InitStateTemplate, FinalStateTemplate
from pipeline.stages.util import default_trace_func, get_output_from_message


class FinalState(FinalStateTemplate):
    pass

class RunState(CommandListState):
    nextState = FinalState
    commandlist = [ ("", '''run:sleep {delay};timeout -s INT 3 python -c "import socket; c = socket.socket(); c.connect(('{peer_ip}', {peer_port}))"'''),
            ("OK:RETVAL(", "quit:")
                ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)
        key = self.in_events.keys()[0]
        # now that we have 2 addresses...
        me = self.getpeername()
        peer = [a for a in self.pipe['addresses'] if a != me][0]
        print 'self.pipe before:', self.pipe
        if len(peer) > 2:
            peer_ts = peer[2]
            delay = 1 - (time.time() - peer_ts)
        else:
            self.pipe['addresses'] = [peer, (me[0], me[1], time.time())]
            print 'self.pipe after adding ts:', self.pipe
            delay = 1
            
        params = {'delay': delay, 'peer_ip': peer[0], 'peer_port': peer[1]+2}
        self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]
        self.emit_event(key, self.in_events.values()[0])  # just forward whatever comes in

class WaitState(CommandListState):
    commandlist = [ ("", "set:dummy:dummy"),
                    ]

    def __init__(self, prevState):
        super(WaitState, self).__init__(prevState)
        params = {}
        self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]

class CheckState(IfElseState):
    consequentState = RunState
    alternativeState = WaitState

    def testfn(self):
        return len(self.pipe['addresses']) > 1

    def __init__(self, prevState):
        super(CheckState, self).__init__(prevState)
       
WaitState.nextState = CheckState

class ActivateState(OnePassState):
    nextState = CheckState
    command = "set:dummy:dummy"

    def __init__(self, prevState):
        super(ActivateState, self).__init__(prevState)
        self.pipe['addresses'] = self.pipe.get('addresses', [])
        self.pipe['addresses'].append(self.getpeername())

class InitState(InitStateTemplate):
    nextState = ActivateState

