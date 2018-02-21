#!/usr/bin/python
# coding=utf-8
import logging


from sprocket.controlling.tracker.machine_state import TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState, IfElseState
from sprocket.config import settings
from sprocket.stages.util import default_trace_func,get_output_from_message, staged_trace_func
from copy import deepcopy 
import time

from sprocket.util.misc import rand_str


class FinalState(OnePassState):
    extra = "(sending quit)"
    expect = None
    command = "quit:"
    nextState = TerminalState

    def __init__(self, prevState):
        super(FinalState, self).__init__(prevState)
        g_end = time.time()
        executionTime = str(g_end - self.local['g_start']) 
        metadata = self.in_events['mod_chunked_link']['metadata']

        self.pipe['benchmarkFile'].write("\nStage:Decode\n")
        self.pipe['benchmarkFile'].write("Lineage:" + str(metadata['lineage']+"\n"))
        self.pipe['benchmarkFile'].write(str(executionTime))

class ConfirmEmitState(OnePassState):
    extra = "(confirm emit)"
    expect = 'OK:EMIT'
    command = None
    nextState = FinalState

    def __init__(self, prevState):
        super(ConfirmEmitState, self).__init__(prevState)

    def post_transition(self):
        metadata = self.in_events['mod_chunked_link']['metadata']

        metacopy = deepcopy(metadata)
        metacopy['end'] = self.in_events['mod_chunked_link']['end']
        self.local['lineage'] = metadata['lineage']

        self.emit_event('frames', {'metadata': metacopy, 'key': self.local['out_key'],
            'nframes':self.local['output_count'],'me':self.local['lineage'],
            'seconds':(self.in_events['mod_chunked_link']['starttime'],
                       self.in_events['mod_chunked_link']['endtime'])})

        return self.nextState(self)  # don't forget this


class TryEmitState(OnePassState):
    extra = "(emit output)"
    expect = None
    command = 'emit:##TMPDIR##/out_0 {out_key}'
    nextState = ConfirmEmitState

    def __init__(self, prevState):
        super(TryEmitState, self).__init__(prevState)
        params = {'out_key': self.local['out_key']}
        self.command = self.command.format(**params)


class CheckOutputState(IfElseState):
    extra = "(check output)"
    expect = 'OK:RETVAL('
    consequentState = TryEmitState
    alternativeState = FinalState

    def testfn(self):
        self.local['output_count'] = int(get_output_from_message(self.messages[-1]))
        return self.local['output_count']>0

    def __init__(self, prevState):
        super(CheckOutputState, self).__init__(prevState)


class RunState(CommandListState):
    extra = "(run)"
    nextState = CheckOutputState

    commandlist = [(None, 'run:mkdir -p ##TMPDIR##/out_0/')
        , ('OK:RETVAL(0)', 'run:./youtube-dl -f "(mp4)" --get-url {URL} 2>/dev/null | head -n1 | xargs -IPLACEHOLDER '
                           './ffmpeg -y -ss {starttime} -i PLACEHOLDER -frames {frames} -f image2 -c:v png '
                           '-start_number 1 ##TMPDIR##/out_0/%08d.png')
        , ('OK:RETVAL(0)', 'run:find ##TMPDIR##/out_0/ -name "*png" | wc -l')
                   # result will be used in next state
                   ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)

        self.local['out_key'] = settings['storage_base']+self.in_events['mod_chunked_link']['metadata']['pipe_id']+'/decode/'+rand_str(16)+'/'

        params = {'starttime': self.in_events['mod_chunked_link']['starttime'],
                  'frames': self.in_events['mod_chunked_link']['frames'],
                  'URL': self.in_events['mod_chunked_link']['key'], 'out_key': self.local['out_key']}
        logging.debug('params: ' + str(params))
        self.commands = [s.format(**params) if s is not None else None for s in self.commands]

class InitState(CommandListState):


    extra = "(init)"
    nextState = RunState
    commandlist = [("OK:HELLO", "seti:nonblock:0")
        #, "run:rm -rf /tmp/*"
        , "run:mkdir -p ##TMPDIR##"
        , None
                   ]


    def __init__(self, prevState, **kwargs):
        super(InitState,self).__init__(prevState, trace_func=kwargs.get('trace_func',(lambda ev,msg,op:staged_trace_func("my_decode",self.in_events['mod_chunked_link']['frames'], self.in_events['mod_chunked_link']['me'],\
                ev,msg,op))),**kwargs)
        logging.debug('in_events: %s', kwargs['in_events'])
        self.local['g_start'] = time.time()




