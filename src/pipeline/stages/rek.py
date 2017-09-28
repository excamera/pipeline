#!/usr/bin/python
import logging
import libmu.util
import uuid

from libmu import tracker, TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState
from pipeline.config import settings
from pipeline.stages.util import default_trace_func,get_output_from_message
from pipeline.stages import InitStateTemplate, GetOutputStateTemplate


class FinalState(TerminalState):
    extra = "(finished)"


class ConfirmEmitState(OnePassState):
    nextState = FinalState
    command= "quit:"
    expect = "OK:EMIT_LIST"

    def __init__(self, prevState):
        super(ConfirmEmitState, self).__init__(prevState)


    def post_transition(self):
            #if Rek failed to find someone
            print "Rek Lineage Emitting:"
            print self.in_events['frames']['metadata']['lineage']
            for i in xrange(len(self.local['key_list'])):
                self.emit_event('frame', {'metadata': self.in_events['frames']['metadata'], 
                    'key': self.local['key_list'][i],'number':i+1,
                    'EOF': i == len(self.local['key_list'])-1, 'type': 'png'})


            if not (i == len(self.local['key_list'])-1):
                print "+===============THIS IS THE PROBLEM AREA"

            return self.nextState(self)  # don't forget this

class TryEmitState(CommandListState):
    extra = "(emit output)"
    nextState = ConfirmEmitState
    commandlist = [(None, "emit_list:{pairs}")
                   ]

    def __init__(self, prevState):
        super(TryEmitState, self).__init__(prevState)
       
        flist = self.local['output']

        # pdb.set_trace()
        self.local['key_list'] = [settings['storage_base'] + self.in_events['frames']['metadata'][
            'pipe_id'] + '/rek/' + str(uuid.uuid4()) for f in flist]
        pairs = [flist[i] + ' ' + self.local['key_list'][i] for i in xrange(len(self.local['key_list']))]
        self.commands = [s.format(**{'pairs': ' '.join(pairs)}) if s is not None else None for s in self.commands]

class GetOutputState(OnePassState):
    extra = "(check output)"
    expect = "OK:RETVAL(0)"
    command = None
    nextState = TryEmitState

    def __init__(self, prevState):
        super(GetOutputState, self).__init__(prevState)

    def post_transition(self):

        output = get_output_from_message(self.messages[-1])
        flist = output.rstrip().split('\n')

        #get rid of non-png objects
        new_flist = []
        for item in flist:
            if '.png' in item:
                new_flist.append(item)

        self.local['output'] = new_flist
        #print self.local['output']

        return self.nextState(self)  # don't forget this

class RunState(CommandListState):
    extra = "(run)"
    nextState = GetOutputState
    commandlist = [ (None, 'run:mkdir -p ##TMPDIR##/in_0/')
                  , ('OK:RETVAL(0)', 'collect:{in_key} ##TMPDIR##/in_0')
                  , ('OK:COLLECT', 'run:mkdir -p ##TMPDIR##/out_0/')
                  , ('OK:RETVAL(0)', 'run: python lambdaRek_opt_mt.py ' +\
                          '"{person}" ##TMPDIR##/in_0/*.png ##TMPDIR##/out_0/ 300 70 5 0.1') #for _opt_mt
                    #check if only txt file is there 
                  , ('OK:RETVAL(0)', 'run:find ##TMPDIR##/out_0/ -type f | sort')
                    #get output in next stage
                    ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)
        self.local['dir'] = '##TMPDIR##/out_0/'
        params = {'in_key': self.in_events['frames']['key'],
                'person':self.in_events['person']['key']}
        logging.debug('params: '+str(params))
        self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]


class InitState(InitStateTemplate):
    nextState = RunState

