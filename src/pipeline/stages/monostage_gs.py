#!/usr/bin/python
import logging

import libmu.util
from libmu import tracker, TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState
from pipeline.config import settings
from pipeline.stages.util import default_trace_func


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

        emit('chunks', {'metadata': self.in_events['video_url']['metadata'], 'key': out_key})


class RunState(CommandListState):
    extra = "(run)"
    nextState = EmitState
    commandlist = [ (None, 'run:mkdir -p ##TMPDIR##/frames/ ##TMPDIR##/gs_frames/ ##TMPDIR##/encoded/ ##TMPDIR##/out_0/')
                  , ('OK:RETVAL(0)', 'run:./ffmpeg -y -ss {starttime} -t {duration} -i "{URL}" -f image2 -c:v png '
                                    '-start_number 1 ##TMPDIR##/frames/%08d.png')
                  , ('OK:RETVAL(0)', 'run:./ffmpeg -start_number 1 -i ##TMPDIR##/frames/%08d.png '
                                     '-vf hue=s=0 -c:a copy -safe 0 -start_number 1 ##TMPDIR##/gs_frames/%08d.png')
                  , ('OK:RETVAL(0)', 'run:./ffmpeg -framerate {fps} -start_number 1 -i ##TMPDIR##/gs_frames/%08d.png '
                                     '-c:v libx264 -pix_fmt yuv420p ##TMPDIR##/encoded/{segment}.mp4')
                  , ('OK:RETVAL(0)', 'run:cd ##TMPDIR##/encoded && $OLDPWD/MP4Box -dash 1000 -rap -segment-name '
                                     'seg_{segment}_ ##TMPDIR##/encoded/{segment}.mp4#video:id=video ##TMPDIR##/encoded/{segment}.mp4#audio:id=audio && cd -')
                  , ('OK:RETVAL(0)', 'run:python amend_m4s.py ##TMPDIR##/encoded/seg_{segment}_1.m4s {segment}')
                  , ('OK:RETVAL(0)', 'run:mv ##TMPDIR##/encoded/00000001_dash.mpd ##TMPDIR##/encoded/00000001_dash_init.mp4 ##TMPDIR##/out_0/; '
                                     'mv ##TMPDIR##/encoded/*m4s ##TMPDIR##/out_0/')
                  , ('OK:RETVAL(0)', 'emit:##TMPDIR##/out_0 {out_key}')
                  , ('OK:EMIT', None)
                    ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState, trace_func=default_trace_func)
        self.emit = prevState.emit
        self.out_key = prevState.out_key

        params = {'starttime': self.in_events['video_url']['starttime'], 'duration': self.in_events['video_url']['duration'],
                  'URL': self.in_events['video_url']['key'], 'out_key': self.out_key, 'fps': self.in_events['video_url']['metadata']['fps'],
                  'segment': '%08d' % int(self.in_events['video_url']['metadata']['lineage'])}

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
        self.out_key = settings['storage_base']+in_events['video_url']['metadata']['pipe_id']+'/monostage_gs/'+libmu.util.rand_str(16)+'/'
        logging.debug('in_events: '+str(in_events)+', emit: '+str(emit))
