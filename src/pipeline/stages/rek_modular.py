#!/usr/bin/python
import logging
import libmu.util
import uuid

from libmu import tracker, TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState
from pipeline.config import settings
from pipeline.stages.util import default_trace_func,get_output_from_message, staged_trace_func
from pipeline.stages import InitStateTemplate, GetOutputStateTemplate

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
        metadata = self.in_events['frames']['metadata']
        self.emit_event('metadata', {'metadata': metadata,
                                       'key': self.in_events['frames']['key'], #pass decode's key
                                       'fps': metadata['fps'],
                                       'lineage':metadata['lineage'],
                                       'nframes':self.in_events['frames']['nframes'],
                                       'boundingbox':self.local['output'],
                                       'rek': self.local['rek'],
                                       'me':metadata['lineage'] }) #whether rek was successful 

        return self.nextState(self)  # don't forget this


class GetOutputState(OnePassState):
    extra = "(check output)"
    expect = "OK:RETVAL(0)"
    command = None
    nextState = EmitState

    def __init__(self, prevState):
        super(GetOutputState, self).__init__(prevState)

    def post_transition(self):

        matchBox = get_output_from_message(self.messages[-1])
   
        self.local['output'] = matchBox

        if str(matchBox) == "False\n":
            self.local['rek'] = False
        else:
            self.local['rek'] = True

        return self.nextState(self)  # don't forget this

class RunState(CommandListState):
    extra = "(run)"
    nextState = GetOutputState
    commandlist = [ (None, 'run:mkdir -p ##TMPDIR##/in_0/')
                  , ('OK:RETVAL(0)', 'run: python lambdaRek_opts3_mt.py ' +\
                          '"{person}" {in_key} {bucket} 300 70 5 0.1 > ##TMPDIR##/in_0/temp.txt')
                  , ('OK:RETVAL(0)', 'run:cat ##TMPDIR##/in_0/temp.txt')
                    #get output in next stage
                    ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)
        self.local['dir'] = '##TMPDIR##/out_0/'
        params = {'in_key': self.in_events['frames']['key'],
                'person':self.in_events['person']['key'],
                'bucket':settings['storage_base'].split('s3://')[1].split('/')[0]}
        logging.debug('params: '+str(params))
        self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]


class InitState(CommandListState):

    nextState = RunState
    extra = "(init)"

    commandlist = [ ("OK:HELLO", "seti:nonblock:0")
                  # , "run:rm -rf /tmp/*"
                  , "run:mkdir -p ##TMPDIR##"
                  , None
                  ]

    def __init__(self, prevState, in_events, emit_event, config):
        super(InitState, self).__init__(prevState, in_events=in_events, emit_event=emit_event, config=config,
trace_func=lambda ev,msg,op:staged_trace_func("Rek_Modular",self.in_events['frames']['nframes'],self.in_events['frames']['me'],ev,msg,op))


