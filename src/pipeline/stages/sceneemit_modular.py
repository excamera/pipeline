#!/usr/bin/python
import json
import logging
import pdb

import math

from libmu import tracker, TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState, IfElseState
from pipeline.config import settings
from pipeline.stages.util import default_trace_func, get_output_from_message, preprocess_config,staged_trace_func
from copy import deepcopy


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

        #extract just the pngs
        self.local['key_list'] = [str(self.in_events['frames']['key'] + \
                str(i).rjust(8,'0') + '.png') for i in range(1,self.in_events['frames']['nframes']+1)]

    def post_transition(self):

        metadata = self.in_events['frames']['metadata']
        fps = metadata['fps'] 

        #put scene change markers 
        t1 = self.local['start_time']
        timeMarkers = []
        for time in self.local['times']:
             timeMarkers.append(math.ceil((time-t1)*fps))
             t1 = time

        for i in xrange(len(self.local['key_list'])):

            sceneChange = False
            if i in timeMarkers:
                sceneChange = True

                print self.local['start_time']
                print "scenechange at: " + str(i)

            #mark the last piece as a scenechange as well
            if metadata['end'] and (i == len(self.local['key_list'])-1):
                sceneChange = True

                print self.local['start_time']
                print "end at: " + str(i)

           
            if (i == (len(self.local['key_list'])-1)):
                print "EOF HERE"
                print "total chunks: " 
                print (i+1)
                print "lineage"
                print self.in_events['frames']['metadata']['lineage']

            self.emit_event('scene_list', {'metadata': self.in_events['frames']['metadata'], 
                    'key': self.local['key_list'][i],'number':i+1,
                    'EOF': i == len(self.local['key_list'])-1, 'type': 'png',
                    'nframes': self.in_events['frames']['nframes'],
                    'me':self.in_events['frames']['metadata']['lineage'],
                    'sceneChange': sceneChange
                    })
                    #TODO: Do i need to include seconds or startime?? i dont think so..

        return self.nextState(self)  # don't forget this
         
        #TODO: Delete the bottom once i am confident i have all the info 
        """
        metadata = self.in_events['frames']['metadata']
        fps = metadata['fps'] 

        metacopy = deepcopy(metadata)
        print "Scenechange"
        print str(self.in_events['frames']['seconds'])
        print metacopy['lineage']

        if len(self.local['times']) >0:
            print self.local['times']

        print "key scene"
        print self.in_events['frames']['key']

        self.emit_event('timestamp',{'output':self.local['times'],
                                    'lineage':metacopy['lineage'],
                                    'metadata':metacopy,
                                    'key': self.in_events['frames']['key'],
                                    'seconds': self.in_events['frames']['seconds'],
                                    'end':metacopy['end'],
                                    'me':metacopy['lineage'],
                                    'nframes':self.in_events['frames']['nframes']})

        return self.nextState(self)  # don't forget this
        """


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

        self.local['times'] = []

        #find all the time stamps of scene changes 
        for line in output[1:]:
            self.local['times'].append( float(self.local['start_time']) + float(line)) 

        return self.nextState(self)  # don't forget this


class RunState(CommandListState):
    extra = "(run)"
    nextState = GetOutputState
    #save all the times
    commandlist = [ (None, 'run:mkdir -p ##TMPDIR##/in_0')
                    ,('OK:RETVAL(0)', 'collect:{in_key} ##TMPDIR##/in_0')
                    , ('OK:COLLECT', 'run: ./ffmpeg -start_number 1 -i ##TMPDIR##/in_0/%08d.png \
                    -vf "select=gt(scene\,0.1),showinfo" -f null - > ##TMPDIR##/out1.txt 2>&1')
                    , ('run: cat ##TMPDIR##/out1.txt')
                    , ("run:grep -Poe '(?<=pts_time:).*(?=pos)|(?<=Duration: ).*(?=, start)'\
                    ##TMPDIR##/out1.txt")  
                   ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)
        self.local['start_time'] = self.in_events['frames']['seconds'][0]
        params = {'in_key':self.in_events['frames']['key']} 
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
    def __init__(self, prevState, **kwargs):
        super(InitState,self).__init__(prevState, trace_func=kwargs.get('trace_func',(lambda ev,msg,op:staged_trace_func("Scenechange",self.in_events['frames']['nframes'], self.in_events['frames']['me'],ev,msg,op))),**kwargs)
        logging.debug('in_events: %s', kwargs['in_events'])


