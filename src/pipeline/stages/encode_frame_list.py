#!/usr/bin/python
import logging
import pdb

from libmu import tracker, TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState
from pipeline.config import settings
from pipeline.stages.util import default_trace_func, get_output_from_message
from pipeline.util.media_probe import get_duration_from_output_lines


class FinalState(TerminalState):
    extra = "(finished)"


class EmitState(CommandListState):
    extra = "(emit output)"
    nextState = FinalState
    commandlist = [ (None, "quit:")
                  ]

    def __init__(self, prevState):
        super(EmitState, self).__init__(prevState)
        self.emit_event('chunks', {'metadata': self.in_events['frame_list']['metadata'], 'key': self.local['out_key']})


class DashifyState(CommandListState):
    extra = "(dashify)"
    nextState = EmitState
    commandlist = [ (None, 'run:cd ##TMPDIR##/temp_0 && $OLDPWD/MP4Box -dash {duration_in_ms} -rap -segment-name '
                                     'seg_{segment}_ ##TMPDIR##/temp_0/{segment}.mp4#video:id=video ##TMPDIR##/temp_0/{segment}.mp4#audio:id=audio && cd -')
                  , ('OK:RETVAL(0)', 'run:python amend_m4s.py ##TMPDIR##/temp_0/seg_{segment}_1.m4s {segment}')
                  , ('OK:RETVAL(0)', 'run:mv ##TMPDIR##/temp_0/00000001_dash.mpd ##TMPDIR##/temp_0/00000001_dash_init.mp4 ##TMPDIR##/out_0/; '
                                     'mv ##TMPDIR##/temp_0/*m4s ##TMPDIR##/out_0/')
                  , ('OK:RETVAL(0)', 'emit:##TMPDIR##/out_0 {out_key}')
                  , ('OK:EMIT', None)
                    ]

    def __init__(self, prevState):
        super(DashifyState, self).__init__(prevState)
        params = {'duration_in_ms': self.local['duration'] * 1000,  # s to ms
                  'segment': '%08d' % int(self.in_events['frame_list']['metadata']['lineage']),
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
    commandlist = [ (None, 'run:mkdir -p ##TMPDIR##/in_0/')
                  , ('OK:RETVAL(0)', 'collect_list:{pair_list}')
                  , ('OK:COLLECT_LIST', 'run:mkdir -p ##TMPDIR##/temp_0/ ##TMPDIR##/out_0')
                  , ('OK:RETVAL(0)', 'run:./ffmpeg -framerate {fps} -start_number 1 -i ##TMPDIR##/in_0/%08d.png '
                                     '-c:v libx264 -pix_fmt yuv420p ##TMPDIR##/temp_0/{segment}.mp4')
                    ]

    def __init__(self, prevState):
        super(EncodeState, self).__init__(prevState)
        pair_list = []
        for i in xrange(len(self.in_events['frame_list']['key_list'])):
            pair_list.append(self.in_events['frame_list']['key_list'][i])
            pair_list.append('##TMPDIR##/in_0/%08d.%s' % (i+1, self.in_events['frame_list']['type']))

        params = {'pair_list': ' '.join(pair_list), 'fps': self.in_events['frame_list']['metadata']['fps'],
                  'segment': '%08d' % int(self.in_events['frame_list']['metadata']['lineage'])}
        logging.debug('params: '+str(params))
        self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]


class InitState(CommandListState):
    extra = "(init)"
    nextState = EncodeState
    commandlist = [ ("OK:HELLO", "seti:nonblock:0")
                  # , "run:rm -rf /tmp/*"
                  , "run:mkdir -p ##TMPDIR##"
                  , None
                  ]

    def __init__(self, prevState, in_events, emit, config):
        super(InitState, self).__init__(prevState, in_events=in_events, emit_event=emit, config=config, trace_func=default_trace_func)
        self.local['out_key'] = settings['storage_base']+in_events['frame_list']['metadata']['pipe_id']+'/encode_frame_list/'
        logging.debug('in_events: '+str(in_events))
