#!/usr/bin/python
import json
import logging
import pdb
import math
import libmu.util
from collections import OrderedDict
from libmu import tracker, TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState, IfElseState
from pipeline.config import settings
from pipeline.stages import InitStateTemplate
from pipeline.stages.util import default_trace_func, get_output_from_message, preprocess_config,staged_trace_func
import datetime

class FinalState(OnePassState):
    extra = "(sending quit)"
    expect = None
    command = "quit:"
    nextState = TerminalState

    def __init__(self, prevState):
        super(FinalState, self).__init__(prevState)


class EmitState(OnePassState):
    extra = "(emit)"
    expect = None
    command = None
    nextState = FinalState

    def __init__(self, prevState):
        super(EmitState, self).__init__(prevState)

    def post_transition(self):
        metadata = self.in_events['video_link']['metadata']
        config = preprocess_config(self.config,
                                   {'fps': metadata['fps']})
        framesperchunk = (config.get('framesperchunk', metadata['fps']))  # default to 1 second chunk
                                                                          # or can do 15 * metadata...
        overlap = config.get('overlap', 0)

        i = 0
        while i * (framesperchunk - overlap) / metadata['fps'] < self.local['duration']:  # actual parallelizing here
            metacopy = metadata.copy()
            starttime = i * (framesperchunk - overlap) / metadata['fps']

            metacopy['lineage'] = str(i+1)
            endChunk = False
            if ((i+1)*(framesperchunk - overlap) / metadata['fps'])>= self.local['duration']:
                endChunk = True

            endtime = starttime+(framesperchunk/int(metadata['fps']))
          
            #for now just making each one second
            starttime = i
            endtime = i+1
            if endtime > self.local['duration']:
                endtime = self.local['duration']

            metacopy['duration'] = self.local['duration']
            self.emit_event('mod_chunked_link', {'metadata': metacopy,
                                       'key': self.in_events['video_link']['key'],
                                       'starttime': starttime,
                                       'endtime': endtime,
                                       'frames': framesperchunk,
                                       'fps': metadata['fps'],
                                       'lineage':i+1,
                                       'end': endChunk,
                                       'me': 1})
            i += 1


        #initialize dictionary that will keep track of how many frames per worker
        #we will have in intermediateConnect_scenes
        #TODO: where should this initialization happen?
        self.pipe['frames_per_worker'] = OrderedDict()

        # set up for benchmarking times 
        pipe= str(self.in_events['video_link']['metadata']['pipe_id'])
        benchmarkFile = open(str("benchmarks/benchmarkFile" + pipe + ".txt"), "w")
        self.pipe['benchmarkFile'] = benchmarkFile

        return self.nextState(self)  # don't forget thiss


class GetOutputState(OnePassState):
    extra = "(check output)"
    expect = 'OK:RETVAL(0'
    command = None
    nextState = EmitState

    def __init__(self, prevState):
        super(GetOutputState, self).__init__(prevState)

    def post_transition(self):
        output = json.loads(get_output_from_message(self.messages[-1]))
        self.local['duration'] = output['duration']
        self.in_events['video_link']['metadata']['fps'] = output['fps']
        return self.nextState(self)  # don't forget this


class RunState(CommandListState):
    extra = "(run)"
    nextState = GetOutputState
    commandlist = [(None, 'run:./youtube-dl -j {link} 2>/dev/null')
                   # output will be used in latter states
                   ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)

        params = {'link': self.in_events['video_link']['key']}
        self.commands = [s.format(**params) if s is not None else None for s in self.commands]


class InitState(CommandListState):
    nextState = RunState
    extra = "(init)"
    commandlist = [ ("OK:HELLO", "seti:nonblock:0")
                  # , "run:rm -rf /tmp/*"
                  , "run:mkdir -p ##TMPDIR##"
                  , None
                  ]   
    def __init__(self, prevState, **kwargs):
       super(InitState,self).__init__(prevState, trace_func=kwargs.get('trace_func',(lambda ev,msg,op:staged_trace_func("Distribute_Link",1,1,ev,msg,op))),**kwargs)
       logging.debug('in_events: %s', kwargs['in_events'])
