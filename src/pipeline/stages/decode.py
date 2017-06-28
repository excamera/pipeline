#!/usr/bin/python
from libmu import tracker, TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState
import libmu.util
import logging

class FinalState(TerminalState):
    extra = "(finished)"


class EmitState(CommandListState):
    extra = "(emit output)"
    nextState = FinalState
    commandlist = [ ('OK:UPLOAD', "quit:")
                  ]

    def __init__(self, prevState):
        super(EmitState, self).__init__(prevState)
        out_queue = prevState.out_queue
        out_key = prevState.out_key

        out_event = {'segment': self.in_events['segment'], 'URL': out_key}
        out_queue['out_0'].put(out_event)


class RunState(CommandListState):
    extra = "(run)"
    nextState = EmitState
    commandlist = [ (None, 'run:./ffmpeg -y -ss {starttime} -t {duration} -i "{URL}" -f image2 -c:v png -r 24 '
                                    '-start_number 1 ##TMPDIR##/%08d.png')
                  , ('OK:RETVAL(0)', 'set:fromfile:##TMPDIR##/00000001.png')
                  , ('OK:', 'set:outkey:{out_key}/00000001.png')
                  , ('OK:', 'upload:')
                    ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)
        self.out_queue = prevState.out_queue
        self.out_key = prevState.out_key

        params = {'starttime': self.in_events['starttime'], 'duration': self.in_events['duration'],
                  'URL': self.in_events['URL'], 'segment': self.in_events['segment'], 'out_key': self.out_key}
        logging.debug('params: '+str(params))
        self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]


class InitState(CommandListState):
    extra = "(configuring)"
    nextState = RunState
    commandlist = [ ("OK:HELLO", "seti:nonblock:0")
                  , "set:bucket:lixiang-pipeline"
                  , "run:rm -rf /tmp/*"
                  , "run:mkdir -p ##TMPDIR##"
                  , None
                  ]

    def __init__(self, prevState, in_events, out_queue):
        super(InitState, self).__init__(prevState, in_events=in_events)
        self.out_queue = out_queue
        self.out_key = 'decode/'+libmu.util.rand_str(16)
        logging.debug('in_events: '+str(in_events)+', out_queue: '+str(out_queue))
