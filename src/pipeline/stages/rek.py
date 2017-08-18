#!/usr/bin/python
import logging
import libmu.util
from libmu import tracker, TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState
from stages.util import default_trace_func


class FinalState(TerminalState):
    extra = "(finished)"


class EmitState(CommandListState):
    extra = "(emit output)"
    nextState = FinalState
    commandlist = [ (None, "quit:")
                  ]

    def __init__(self, prevState):
        super(EmitState, self).__init__(prevState, trace_func=default_trace_func)
        emit = prevState.emit
        out_key = prevState.out_key

        emit('frames', {'metadata': self.in_events['frames']['metadata'], 'key': out_key})


class RunState(CommandListState):
    extra = "(run)"
    nextState = EmitState
    commandlist = [ (None, 'run:mkdir -p ##TMPDIR##/in_0/')
                  , ('OK:RETVAL(0)', 'collect:{in_key} ##TMPDIR##/in_0')
                  , ('OK:COLLECT', 'run:mkdir -p ##TMPDIR##/out_0/')
                  , ('OK:RETVAL(0)','run:python lambdaRek_mt.py ' +\
			'"{person}.jpg" ##TMPDIR##/in_0/*.png ##TMPDIR##/out_0/ 1000 60') #NOTE "{person}.jpg" 
                  , ('OK:RETVAL(0)', 'emit:##TMPDIR##/out_0 {out_key}')
                  , ('OK:EMIT', None)
                    ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState, trace_func=default_trace_func)
        self.emit = prevState.emit
        self.out_key = prevState.out_key

        params = {'in_key': self.in_events['frames']['key'], 'out_key': self.out_key,
		'person': str(self.in_events['frames']['metadata']['configs']['googleFace']['person'])}

        logging.debug('params: '+str(params))
        self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]


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
	self.person = str(self.in_events['frames']['metadata']['configs']['googleFace']['person'])
        self.out_key = 's3://liz-pipeline/'+in_events['frames']['metadata']['pipe_id']+'/rek/'+libmu.util.rand_str(16)+'/'
        logging.debug('in_events: '+str(in_events)+', emit: '+str(emit))
