#!/usr/bin/python
import json
import logging
import pdb

import math

from libmu import tracker, TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState, IfElseState
from pipeline.config import settings
from pipeline.stages.util import default_trace_func, get_output_from_message, preprocess_config
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

    def post_transition(self):

        #first extract fps and total duration
        first_output = self.in_events['timestamp']['combined_events'][0]['timestamp']['output']


        self.local['fps'] = float(first_output[1])
        self.local['times'] = []
        endChunk = False

        for i in range (0,len(self.in_events['timestamp']['combined_events'])):
            output = self.in_events['timestamp']['combined_events'][i]

            #if the second is a 0, append a 0
            if output['timestamp']['seconds'][0] == 0:
                self.local['times'].append(0)
                lineage = output['timestamp']['lineage']
            #if the chunk is end, eventually add the duration
            if str(output['timestamp']['end']) == str(True):
                endChunk = True
            if len(output['timestamp']['output']) > 3:
                if i == 0: #if this is the first event, only append the last scene change
                    self.local['times'].append(float(output['timestamp']['output'][-1])) 
                    lineage = output['timestamp']['lineage']
                    print "Lineage is:"
                    print lineage
                else:
                    #print "-----We have encountered a larger time\n"
                    #print output
                    #print "--------\n"
                    for time in output['timestamp']['output'][3:]:
                        self.local['times'].append(float(time)) 

        if endChunk:
            # save self.local['duration'] to be a list of times.
            duration = first_output[0]

            #convert the duration to seconds 
            ftr = [3600,60,1]
            msec = duration.split('.')[1]
            time = duration.split('.')[0]
            self.local['duration'] = float(str(sum([a*b for a,b in \
                                zip(ftr, map(int, time.split(':')))])) + '.'+str(msec))
            self.local['times'].append(self.local['duration'])

        ############

        metadata = self.in_events['timestamp']['metadata']
        fps = int(self.local['fps'])

        #remove redundant times
        self.local['times'] = list(set(self.local['times']))

        #sort the times since who knows what order they are in
        self.local['times'].sort()

        #TODO: Eventually remove the printing
        print self.local['times']



        #emit scene change chunks
        for startime in range(0,len(self.local['times'])-1):
            metacopy = deepcopy(metadata)
            starttime = self.local['times'][startime]
            frames = math.ceil((self.local['times'][startime+1]-self.local['times'][startime])*fps)


            averageFramesNumber = \
                    int((float(float(frames)/float(fps))/\
                    float(math.ceil(float(frames)/float(fps))))*float(fps))

            for chunk in range(0,int(math.ceil(frames/fps))):
                endChunk = False

                #for the last chunk
                if (startime == len(self.local['times'])-2) and \
                        (chunk == int(math.ceil(frames/fps)-1)):
                                endChunk = True

                #TODO: Make lineage the first second in the chunks?
                #Figure out!
                localChunkStarttime = starttime + chunk*(float(averageFramesNumber)/float(fps))
                metacopy['lineage'] = lineage

                self.emit_event('my_chunked_link', {'metadata': metacopy,
                                       'key': self.in_events['timestamp']['key'],
                                       'starttime':localChunkStarttime,
                                       'frames': averageFramesNumber,
                                       'mynumber':lineage,
                                       'endChunk':endChunk}) #TODO:used to be frames.

                lineage+=1

                """ 
                print "\nlineage: " + str(lineage-1)
                print "\nendChunk: " + str(endChunk)
                print "\nstarttime: " + str(localChunkStarttime)
                print "\naverage Frames: " + str(averageFramesNumber)
                """
        return self.nextState(self)  # don't forget this




class RunState(CommandListState):
    extra = "(run)"
    nextState = EmitState
    commandlist = [] #no commands for lambda

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)

        #params = {'link': self.in_events['video_link']['key']}
        #self.commands = [s.format(**params) if s is not None else None for s in self.commands]


class InitState(CommandListState):
    extra = "(init)"
    nextState = EmitState #dont need to run anything on lambda
    commandlist = [ ("OK:HELLO", "seti:nonblock:0")
                  , "run:rm -rf /tmp/*"
                  , "run:mkdir -p ##TMPDIR##"
                  , None
                  ]

    def __init__(self, prevState, in_events, emit_event, config):
        super(InitState, self).__init__(prevState, in_events=in_events, emit_event=emit_event, config=config, trace_func=default_trace_func)
        logging.debug('in_events: '+str(in_events))
