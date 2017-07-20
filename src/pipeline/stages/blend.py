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

        emit('frames', {'metadata': self.in_events['frames_0']['metadata'], 'key': out_key})


class RunState(CommandListState):
    extra = "(run)"
    nextState = EmitState
    commandlist = [ (None, 'run:mkdir -p ##TMPDIR##/in_0/ ##TMPDIR##/in_1/ ##TMPDIR##/out_0/')
                  , ('OK:RETVAL(0)', 'collect:{in_key_0} ##TMPDIR##/in_0')
                  , ('OK:COLLECT', 'collect:{in_key_1} ##TMPDIR##/in_1')
                  , ('OK:COLLECT', 'run:./ffmpeg -y -i ##TMPDIR##/in_0/%08d.png -i ##TMPDIR##/in_1/%08d.png -filter_complex '
                                   '\'scale2ref[1:v][0:v]; [0:v][1:v]blend=all_mode=average\' ##TMPDIR##/out_0/%08d.png')
                  , ('OK:RETVAL(0)', 'emit:##TMPDIR##/out_0 {out_key}')
                  , ('OK:EMIT', None)
                    ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState, trace_func=default_trace_func)
        self.emit = prevState.emit
        self.out_key = prevState.out_key

        params = {'in_key_0': self.in_events['frames_0']['key'], 'in_key_1': self.in_events['frames_1']['key'],
                  'out_key': self.out_key}
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
        self.out_key = 's3://lixiang-pipeline/'+in_events['frames_0']['metadata']['pipe_id']+'/blend/'+libmu.util.rand_str(16)+'/'
        logging.debug('in_events: '+str(in_events)+', emit: '+str(emit))
