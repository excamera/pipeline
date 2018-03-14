#!/usr/bin/python
# coding=utf-8
import logging

from sprocket.util.misc import rand_str
from sprocket.controlling.tracker.machine_state import TerminalState, CommandListState, OnePassState, IfElseState
from sprocket.config import settings
from sprocket.stages import InitStateTemplate, CreateTarStateTemplate
from sprocket.stages.util import default_trace_func, get_output_from_message


class FinalState(OnePassState):
    extra = "(sending quit)"
    expect = None
    command = "quit:"
    nextState = TerminalState

    def __init__(self, prevState):
        super(FinalState, self).__init__(prevState)


class ConfirmEmitState(OnePassState):
    extra = "(confirm emit)"
    expect = 'OK:EMIT'
    command = None
    nextState = FinalState

    def __init__(self, prevState):
        super(ConfirmEmitState, self).__init__(prevState)

    def post_transition(self):
        self.emit_event('frames', {'metadata': self.in_events['chunks']['metadata'], 'key': self.local['out_key']
            , 'nframes': self.local['output_count']})

        #for smart serialization
        lineage = self.in_events['chunks']['metadata']['lineage']
        return self.nextState(self)  # don't forget this


class TryEmitState(CommandListState):
    extra = "(emit output)"
    commandlist = [  (None, 'emit:##TMPDIR##/out_0 {out_key}')
    ]
    nextState = ConfirmEmitState

    def __init__(self, prevState):
        super(TryEmitState, self).__init__(prevState)
        params = {'out_key': self.local['out_key']}
        self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]


class CreateTarState(CreateTarStateTemplate):
    nextState = TryEmitState
    tar_dir = '##TMPDIR##/out_0/'


class CheckOutputState(IfElseState):
    extra = "(check output)"
    expect = 'OK:RETVAL('
    consequentState = CreateTarState if settings.get('use_tar') else TryEmitState
    alternativeState = FinalState

    def testfn(self):
        self.local['output_count'] = int(get_output_from_message(self.messages[-1]))
        self.in_events['chunks']['metadata']['fps'] = self.in_events['chunks']['metadata'].get('fps', self.local['output_count'])
        return self.local['output_count'] > 0

    def __init__(self, prevState):
        super(CheckOutputState, self).__init__(prevState)


class GetFramerateState(OnePassState):
    nextState = CheckOutputState
    expect = 'OK:RETVAL(0)'
    command = 'run:find ##TMPDIR##/out_0/ -name "*png" | wc -l'

    def __init__(self, prevState):
        super(GetFramerateState, self).__init__(prevState)

    def post_transition(self):
        output = get_output_from_message(self.messages[-1])
        fields = filter(lambda s: 'fps' in s, output.split(','))
        if len(fields) == 0:
            # we can only guess later...
            logging.error("unable to obtain fps, guessing. output: %s, fps fields: %s" % (output, fields))
        else:
            try:
                self.in_events['chunks']['metadata']['fps'] = float(fields[0].split()[0])
            except:
                logging.error("unable to obtain fps, guessing. output: %s, fps fields: %s" % (output, fields))
        return self.nextState(self)  # don't forget this


class RunState(CommandListState):
    extra = "(run)"
    nextState = GetFramerateState
    commandlist = [ (None, 'run:mkdir -p ##TMPDIR##/in_0/ ##TMPDIR##/out_0/')
                  , ('OK:RETVAL(0)', 'collect:{key} ##TMPDIR##/in_0')
                  , ('OK:COLLECT', 'run:./ffmpeg -y -i `find ##TMPDIR##/in_0/ -name "*.mp4"` -f image2 -c:v png '
                                    '-start_number 1 ##TMPDIR##/out_0/%08d.png 2>&1|grep fps|head -n1')
                    ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState, trace_func=default_trace_func)
        if settings.get('hash_bucket'):
            self.local['out_key'] = settings['temp_storage_base'] + rand_str(1) + '/' + rand_str(16) + '/'
        else:
            self.local['out_key'] = settings['storage_base'] + rand_str(16) + '/'
        params = {'key': self.in_events['chunks']['key'], 'out_key': self.local['out_key']}
        logging.debug('params: '+str(params))
        self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]


class InitState(InitStateTemplate):
    nextState = RunState

    def __init__(self, prevState, **kwargs):
        super(InitState, self).__init__(prevState, **kwargs)
        self.trace_func = lambda ev, msg, op: default_trace_func(ev, msg, op, stage='decode')
