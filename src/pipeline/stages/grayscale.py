#!/usr/bin/python
import logging
import libmu.util
from libmu import tracker, TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState
from pipeline.config import settings
from pipeline.stages import InitStateTemplate
from pipeline.stages.util import default_trace_func


class FinalState(TerminalState):
    extra = "(finished)"


class EmitState(CommandListState):
    extra = "(emit output)"
    nextState = FinalState
    commandlist = [ (None, "quit:")
                  ]

    def __init__(self, prevState):
        super(EmitState, self).__init__(prevState)
        self.emit_event('frames', {'metadata': self.in_events['frames']['metadata'], 'key': self.local['out_key']})
        # self.emit_event('frames', {'metadata': self.in_events['frames']['metadata'], 'keys': [(self.local['out_key'],
        #                 (1, self.in_events['frames']['nframes'], self.in_events['frames']['nframes']))]})  # TODO: better replace with real output number


class RunState(CommandListState):
    extra = "(run)"
    nextState = EmitState
    commandlist = [ (None, 'run:mkdir -p ##TMPDIR##/in_0/ ##TMPDIR##/in_1/')
                  , ('OK:RETVAL(0)', 'collect:{in_key} ##TMPDIR##/in_1')
                  , ('OK:COLLECT', 'run:tar -xvf ##TMPDIR##/in_1/files.tar -C ##TMPDIR##/in_0/')
                  , ('OK:RETVAL(0)', 'run:mkdir -p ##TMPDIR##/out_0/')
                  , ('OK:RETVAL(0)', 'run:./ffmpeg -start_number 1 -i ##TMPDIR##/in_0/%08d.png '
                                     '-vf hue=s=0 -c:a copy -safe 0 -start_number 1 ##TMPDIR##/out_0/%08d.png')
                  , ('OK:RETVAL(0)', 'emit:##TMPDIR##/out_0 {out_key}')
                  , ('OK:EMIT', None)
                    ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)
        self.local['out_key'] = settings['storage_base']+self.in_events['frames']['metadata']['pipe_id']+'/grayscale/'+libmu.util.rand_str(16)+'/'

        params = {'in_key': self.in_events['frames']['key'], 'out_key': self.local['out_key']}
        logging.debug('params: '+str(params))
        self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]


class InitState(InitStateTemplate):
    nextState = RunState
