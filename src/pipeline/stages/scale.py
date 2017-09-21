#!/usr/bin/python
import logging
import pdb
import uuid

import libmu.util
from libmu import tracker, TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState
from pipeline.config import settings
from pipeline.stages.util import default_trace_func
from pipeline.stages import InitStateTemplate, GetOutputStateTemplate

"""for each chunk, retain a random number of (continuous) frames while cut other frames"""


class FinalState(TerminalState):
    extra = "(finished)"


class ConfirmEmitState(OnePassState):
    nextState = FinalState
    expect = "OK:EMIT_LIST"
    command = "quit:"

    def __init__(self, prevState):
        super(ConfirmEmitState, self).__init__(prevState)

    def post_transition(self):
        scale = float(self.config.get('scale', 1))
        k_list = self.local['key_list']
        outlist = [k_list[int(i / scale)] for i in range(0, int(len(k_list) * scale) + 1) if
                   int(i / scale) < len(k_list)]
        for i in xrange(len(outlist)):
            self.emit_event('frame', {'metadata': self.in_events['frames']['metadata'], 'key': outlist[i],
                                      'number': i+1, 'EOF': i == len(outlist) - 1, 'type': 'png'})
        return self.nextState(self)  # don't forget this


class TryEmitState(CommandListState):
    extra = "(emit output)"
    nextState = ConfirmEmitState
    commandlist = [(None, "emit_list:{pairs}")
                   ]

    def __init__(self, prevState):
        super(TryEmitState, self).__init__(prevState)
        flist = self.local['output'].rstrip().split('\n')
        # pdb.set_trace()
        self.local['key_list'] = [settings['storage_base'] + self.in_events['frames']['metadata'][
            'pipe_id'] + '/random_cut/' + str(uuid.uuid4()) for f in flist]
        pairs = [flist[i] + ' ' + self.local['key_list'][i] for i in xrange(len(self.local['key_list']))]
        self.commands = [s.format(**{'pairs': ' '.join(pairs)}) if s is not None else None for s in self.commands]


class GetOutput(GetOutputStateTemplate):
    nextState = TryEmitState


class RunState(CommandListState):
    extra = "(run)"
    nextState = GetOutput
    commandlist = [(None, 'run:mkdir -p ##TMPDIR##/in_0/')
        , ('OK:RETVAL(0)', 'collect:{in_key} ##TMPDIR##/in_0')
        , ('OK:COLLECT', 'run:mkdir -p ##TMPDIR##/out_0/')
        , ('OK:RETVAL(0)', 'run:cp ##TMPDIR##/in_0/* ##TMPDIR##/out_0/')  # we get random # of frames
        , ('OK:RETVAL(0)', 'run:find ##TMPDIR##/out_0/ -type f | sort')
                   ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)
        self.local['dir'] = '##TMPDIR##/out_0/'
        params = {'in_key': self.in_events['frames']['key']}
        logging.debug('params: ' + str(params))
        self.commands = [s.format(**params) if s is not None else None for s in self.commands]


class InitState(InitStateTemplate):
    nextState = RunState
