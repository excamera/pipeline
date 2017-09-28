#!/usr/bin/python
import json
import logging
import pdb

import math

from libmu import tracker, TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState, IfElseState
from pipeline.config import settings
from pipeline.stages.util import default_trace_func, get_output_from_message, preprocess_config


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

        #don't emit if no scenes

        metadata = self.in_events['chunked_link_forScene']['metadata']
        fps = self.in_events['chunked_link_forScene']['fps'] 
        #config = preprocess_config(metadata['configs']['parlink'],
        #                           {'fps': metadata['fps']})
        #framesperchunk = config.get('framesperchunk', metadata['fps'])  # default to 1 second chunk
        #overlap = config.get('overlap', 0)

        metacopy = metadata.copy()
        self.emit_event('timestamp',{'output':self.local['output'],
                                    'lineage':self.in_events['chunked_link_forScene']['lineage'],
                                    'metadata':metacopy,
                                    'key': self.in_events['chunked_link_forScene']['key'],
                                    'seconds': (self.in_events['chunked_link_forScene']['starttime'],
                                        self.in_events['chunked_link_forScene']['starttime']+1),
                                    'end':self.in_events['chunked_link_forScene']['end']})



        return self.nextState(self)  # don't forget this


class GetOutputState(OnePassState):
    extra = "(check output)"
    expect = 'OK:RETVAL(0)'
    command = None
    nextState = EmitState

    def __init__(self, prevState):
        super(GetOutputState, self).__init__(prevState)

    def post_transition(self):
        output = (get_output_from_message(self.messages[-1])).splitlines()
        self.local['output'] = output

        # if no scene detection
        if len(output) == 3:
            return self.nextState(self)  # don't forget this

        self.in_events['chunked_link_forScene']['fps']= float(output[1])

        

        #TODO: Figure out how to deal with first chunk
        #self.local['times'] = [0] #start at 0 seconds

        self.local['times'] = []



        #find all the time stamps of scene changes 
        for line in output[3:]:
            self.local['times'].append(float(line)) 

        # save self.local['duration'] to be a list of times.
        duration = output[0]

        #TODO: Figure otu how to deal with last chunk
        #convert the duration to seconds 
        ftr = [3600,60,1]
        msec = duration.split('.')[1]
        time = duration.split('.')[0]
        self.local['duration'] = float(str(sum([a*b for a,b in \
            zip(ftr, map(int, time.split(':')))])) + '.'+str(msec))
        #self.local['times'].append(self.local['duration'])

        return self.nextState(self)  # don't forget this


class RunState(CommandListState):
    extra = "(run)"
    nextState = GetOutputState
    #save all the times
    #TODO: this youtube-dl can probs me moved earlier to do only once and passed in
    commandlist = [ (None, 'run: ./youtube-dl --get-url {link} -f "(mp4)"  2>/dev/null |\
                    head -n1 | xargs -IPLACEHOLDER ./ffmpeg \
                 -i \'PLACEHOLDER\' \
                  -vf "trim={starttime}:{totime}, select=gt(scene\,0.2),showinfo" -'\
                          'f null - > ##TMPDIR##/out1.txt 2>&1')
                    ,("run:grep -Poe '(?<=pts_time:).*(?=pos)|(?<=Duration: ).*(?=, start)|(?<=kb/s, ).*(?= fps)'\
                    ##TMPDIR##/out1.txt")  
                   ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)

        params = {'starttime': self.in_events['chunked_link_forScene']['starttime'],
                  'frames': self.in_events['chunked_link_forScene']['frames'],
                  'link': self.in_events['chunked_link_forScene']['key'],
                  'fps':self.in_events['chunked_link_forScene']['fps'],
                  'totime':str(float(self.in_events['chunked_link_forScene']['starttime'])+1) }
        logging.debug('params: ' + str(params))
        self.commands = [s.format(**params) if s is not None else None for s in self.commands]


class InitState(CommandListState):
    extra = "(init)"
    nextState = RunState
    commandlist = [ ("OK:HELLO", "seti:nonblock:0")
                  , "run:rm -rf /tmp/*"
                  , "run:mkdir -p ##TMPDIR##"
                  , None
                  ]

    def __init__(self, prevState, in_events, emit_event,config):
        super(InitState, self).__init__(prevState, in_events=in_events, emit_event=emit_event,config=config, trace_func=default_trace_func)
        logging.debug('in_events: '+str(in_events))
