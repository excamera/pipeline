#!/usr/bin/python
import logging
import pdb


from sprocket.controlling.tracker.machine_state import TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState
from sprocket.config import settings
from sprocket.stages import InitStateTemplate, ExtractTarStateTemplate
from sprocket.stages.util import default_trace_func, get_output_from_message
from sprocket.util.media_probe import get_duration_from_output_lines


class FinalState(TerminalState):
    extra = "(finished)"


class EmitState(CommandListState):
    extra = "(emit output)"
    nextState = FinalState
    commandlist = [ (None, "quit:")
                  ]

    def __init__(self, prevState):
        super(EmitState, self).__init__(prevState)
        self.emit_event('chunks', {'metadata': self.in_events['frames']['metadata'], 'key': self.local['out_key'],
                                   'duration': self.local['duration']})


class DashifyState(CommandListState):
    extra = "(dashify)"
    nextState = EmitState
    commandlist = [ (None, 'run:cd ##TMPDIR##/temp_0 && $OLDPWD/MP4Box -dash {duration_in_ms} -rap -segment-name '
                                     'seg_{segment}_ ##TMPDIR##/temp_0/{segment}.mp4#video:id=video ##TMPDIR##/temp_0/{segment}.mp4#audio:id=audio && cd -')
                  , ('OK:RETVAL(0)', 'run:python amend_m4s.py ##TMPDIR##/temp_0/seg_{segment}_1.m4s {segment} {relative_duration}')
                  , ('OK:RETVAL(0)', 'run:mv ##TMPDIR##/temp_0/00000001_dash.mpd ##TMPDIR##/temp_0/00000001_dash_init.mp4 ##TMPDIR##/out_0/; '
                                     'mv ##TMPDIR##/temp_0/*m4s ##TMPDIR##/out_0/')
                  , ('OK:RETVAL(0)', 'emit:##TMPDIR##/out_0 {out_key}')
                  , ('OK:EMIT', None)
                    ]

    def __init__(self, prevState):
        super(DashifyState, self).__init__(prevState)
        self.local['out_key'] = settings['storage_base']+self.in_events['frames']['metadata']['pipe_id']+'/encode_to_dash/'
        segment = '%08d' % int(self.in_events['frames']['metadata']['lineage'])
        chunk_duration = self.in_events['frames']['metadata'].get('chunk_duration')
        relative_duration = self.local['duration'] / chunk_duration if chunk_duration else ''
        params = {'duration_in_ms': self.local['duration'] * 1000,  # s to ms
                  'segment': segment,
                  'relative_duration': relative_duration,
                  'out_key': self.local['out_key']}
        logging.debug('params: '+str(params))
        self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]


class GetDurationState(OnePassState):
    extra = "(get duration)"
    nextState = DashifyState
    expect = 'OK:RETVAL(0)'
    command = None

    def __init__(self, prevState):
        super(GetDurationState, self).__init__(prevState)

    def post_transition(self):
        self.local['duration'] = get_duration_from_output_lines(get_output_from_message(self.messages[-1]).split('\n'))
        return self.nextState(self)


class EncodeState(CommandListState):
    extra = "(encode)"
    nextState = GetDurationState
    commandlist = [ (None, 'run:mkdir -p ##TMPDIR##/temp_0/ ##TMPDIR##/out_0')
                  , ('OK:RETVAL(0)', 'run:./ffmpeg -framerate {fps} -start_number 1 -i ##TMPDIR##/in_0/%08d.png '
                                     '-c:v libx264 -pix_fmt yuv420p ##TMPDIR##/temp_0/{segment}.mp4')
                    ]

    def __init__(self, prevState):
        super(EncodeState, self).__init__(prevState)

        params = {'fps': self.in_events['frames']['metadata']['fps'],
                  'segment': '%08d' % int(self.in_events['frames']['metadata']['lineage'])}
        logging.debug('params: '+str(params))
        self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]


class ExtractTarState(ExtractTarStateTemplate):
    tar_dir = '##TMPDIR##/in_0/'
    nextState = EncodeState


class CollectState(CommandListState):
    nextState = ExtractTarState if settings.get('use_tar') else EncodeState
    commandlist = [
        (None, 'run:mkdir -p ##TMPDIR##/in_0/')
        , ('OK:RETVAL(0)', 'collect:{in_key} ##TMPDIR##/in_0')
        , ('OK:COLLECT', 'run:ls ##TMPDIR##/in_0/*png')
        , ('OK:RETVAL(0)', None)
    ]

    def __init__(self, prevState):
        super(CollectState, self).__init__(prevState)
        params = {'in_key': self.in_events['frames']['key']}
        self.commands = [s.format(**params) if s is not None else None for s in self.commands]


class InitState(InitStateTemplate):
    nextState = CollectState

    def __init__(self, prevState, **kwargs):
        super(InitState, self).__init__(prevState, **kwargs)
        self.trace_func = lambda ev, msg, op: default_trace_func(ev, msg, op, stage='encode')

