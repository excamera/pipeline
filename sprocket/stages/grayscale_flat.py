#!/usr/bin/python
import logging

from sprocket.controlling.tracker.machine_state import TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState
from sprocket.config import settings
from sprocket.stages import InitStateTemplate, ExtractTarStateTemplate, CreateTarStateTemplate, FinalStateTemplate,GetOutputStateTemplate 
from sprocket.stages.util import default_trace_func
from sprocket.util.misc import rand_str


class FinalState(FinalStateTemplate):
    pass

class ConfirmEmitState(OnePassState):
    nextState = FinalState
    expect = "OK:EMIT_LIST"
    command = None

    def __init__(self, prevState):
        super(ConfirmEmitState, self).__init__(prevState)

    def post_transition(self):
        for i in xrange(len(self.local['key_list'])):
            self.emit_event('frame', {'metadata': self.in_events['frames']['metadata'], 'key': self.local['key_list'][i],
                                      'number': i+1, 'EOF': i == len(self.local['key_list']) -1, 'type': 'png'})
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
            'pipe_id'] + '/grayscale_flat/' + str(rand_str(16)) for f in flist]
        pairs = [flist[i] + ' ' + self.local['key_list'][i] for i in xrange(len(self.local['key_list']))]
        self.commands = [s.format(**{'pairs': ' '.join(pairs)}) if s is not None else None for s in self.commands]



class CreateTarState(CreateTarStateTemplate):
    nextState = TryEmitState
    tar_dir = '##TMPDIR##/out_0'

class GetOutput(GetOutputStateTemplate):
    nextState = CreateTarState if settings.get('use_tar') else TryEmitState

class RunState(CommandListState):

    nextState = GetOutput
    extra = "(run)"
    commandlist = [ (None, 'run:mkdir -p ##TMPDIR##/out_0/')
                  , ('OK:RETVAL(0)', 'run:./ffmpeg -start_number 1 -i ##TMPDIR##/in_0/%08d.png '
                                     '-vf hue=s=0 -c:a copy -safe 0 -start_number 1 ##TMPDIR##/out_0/%08d.png')

                  , ('OK:RETVAL(0)', 'run:find ##TMPDIR##/out_0/ -type f | sort')
                    ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)

        params = {'in_key': self.in_events['frames']['key']}
        logging.debug('params: '+str(params))
        self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]


class ExtractTarState(ExtractTarStateTemplate):
    tar_dir = '##TMPDIR##/in_0/'
    nextState = RunState


class CollectState(CommandListState):
    nextState = ExtractTarState if settings.get('use_tar') else RunState
    commandlist = [
        (None, 'run:mkdir -p ##TMPDIR##/in_0/')
        , ('OK:RETVAL(0)', 'collect:{in_key} ##TMPDIR##/in_0')
        , ('OK:COLLECT', None)
    ]

    def __init__(self, prevState):
        super(CollectState, self).__init__(prevState)
        params = {'in_key': self.in_events['frames']['key']}
        self.commands = [s.format(**params) if s is not None else None for s in self.commands]


class InitState(InitStateTemplate):
    nextState = CollectState

    def __init__(self, prevState, **kwargs):
        super(InitState, self).__init__(prevState, **kwargs)
        self.trace_func = lambda ev, msg, op: default_trace_func(ev, msg, op, stage='grayscale')
