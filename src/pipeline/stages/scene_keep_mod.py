#!/usr/bin/python
import logging
import libmu.util
import uuid

from libmu import tracker, TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState
from pipeline.config import settings
from pipeline.stages.util import default_trace_func,get_output_from_message, staged_trace_func
from pipeline.stages import InitStateTemplate, GetOutputStateTemplate


class FinalState(TerminalState):
    extra = "(sending quit)"
    expect = None
    command = "quit:"
    nextState = TerminalState

    def __init__(self, prevState):
        super(FinalState, self).__init__(prevState)

class ConfirmNoRekEmitState(OnePassState):
    nextState = FinalState
    command= 'quit:'

    def __init__(self, prevState):
        super(ConfirmNoRekEmitState, self).__init__(prevState)

    def post_transition(self):

        #emit a dumb empty event
        self.emit_event('frame', {'metadata':self.in_events['metadata']['metadata'],
                    'key': None, 'number': 1, 'type':'png',
                    'EOF':True,'Empty':True,
                    'me': self.in_events['metadata']['lineage']})

        return self.nextState(self)  # don't forget this

class ConfirmRekEmitState(OnePassState):
    nextState = FinalState
    command= 'quit:'

    def __init__(self, prevState):
        super(ConfirmRekEmitState, self).__init__(prevState)

    def post_transition(self):

            for i in xrange(len(self.local['key_list'])):
                self.emit_event('frame', {'metadata': self.in_events['metadata']['metadata'], 
                    'key': self.local['key_list'][i],'number':i+1,
                    'EOF': i == len(self.local['key_list'])-1, 'type': 'png',
                    'me': self.in_events['metadata']['lineage']})

            return self.nextState(self)  # don't forget this

#keep all the frames unmodified and pass them through
class TryRekEmitState(OnePassState):
    extra = "(emit output)"
    nextState = ConfirmRekEmitState

    def __init__(self, prevState):
        super(TryRekEmitState, self).__init__(prevState)
       
        #extract just the pngs
        self.local['key_list'] = [str(self.in_events['metadata']['key'] + \
                str(i).rjust(8,'0') + '.png') for i in range(1,self.in_events['metadata']['nframes']+1)]


#figure out if were even going to proceed with this stage
class InitState(CommandListState):
    extra = "(init)"
    commandlist = [("OK:HELLO", "seti:nonblock:0")
        # , "run:rm -rf /tmp/*"
        , "run:mkdir -p ##TMPDIR##"
        , None
                   ]

    def __init__(self, prevState, in_events, emit_event, config):

        super(InitState, self).__init__(prevState, in_events=in_events, emit_event=emit_event, config=config, trace_func=lambda ev,msg,op:staged_trace_func("Scene_Keep",self.in_events['metadata']['nframes'],self.in_events['metadata']['me'],ev,msg,op))

        if self.in_events['metadata']['rek'] == False:
            print "===FALSE"
            self.nextState = ConfirmNoRekEmitState
        else:
            print "===TRUE"
            self.nextState = TryRekEmitState

        logging.debug('in_events: ' + str(in_events))

