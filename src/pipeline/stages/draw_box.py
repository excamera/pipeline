#!/usr/bin/python
import logging
import libmu.util
import uuid

from libmu import tracker, TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState
from pipeline.config import settings
from pipeline.stages.util import default_trace_func,get_output_from_message, staged_trace_func
from pipeline.stages import InitStateTemplate, GetOutputStateTemplate


class FinalState(OnePassState):
    extra = "(sending quit)"
    expect = None
    command = "quit:"
    nextState = TerminalState

    def __init__(self, prevState):
        super(FinalState, self).__init__(prevState)

class ConfirmRekEmitState(OnePassState):
    nextState = FinalState
    expect = "OK:EMIT_LIST"

    def __init__(self, prevState):
        super(ConfirmRekEmitState, self).__init__(prevState)

    def post_transition(self):

            for i in xrange(len(self.local['key_list'])):
                self.emit_event('frame', {'metadata': self.in_events['metadata']['metadata'], 
                    'key': self.local['key_list'][i],'number':i+1,
                    'EOF': i == len(self.local['key_list'])-1, 'type': 'png',
                    'nframes': self.in_events['metadata']['nframes']})

            return self.nextState(self)  # don't forget this

class ConfirmNoRekEmitState(OnePassState):
    nextState = FinalState

    def __init__(self, prevState):
        super(ConfirmNoRekEmitState, self).__init__(prevState)

    def post_transition(self):

            for i in xrange(len(self.local['key_list'])):
                self.emit_event('frame', {'metadata': self.in_events['metadata']['metadata'], 
                    'key': self.local['key_list'][i],'number':i+1,
                    'EOF': i == len(self.local['key_list'])-1, 'type': 'png',
                    'nframes': self.in_events['metadata']['nframes'],
                    'me':self.in_events['metadata']['lineage']})

            return self.nextState(self)  # don't forget this

class TryNoRekEmitState(OnePassState):
    extra = "(emit output)"
    nextState = ConfirmNoRekEmitState

    def __init__(self, prevState):
        super(TryNoRekEmitState, self).__init__(prevState)
       
        #extract just the pngs
        self.local['key_list'] = [str(self.in_events['metadata']['key'] + \
                str(i).rjust(8,'0') + '.png') for i in range(1,self.in_events['metadata']['nframes']+1)]



class TryRekEmitState(CommandListState):
    extra = "(emit output)"
    nextState = ConfirmRekEmitState
    commandlist = [(None, "emit_list:{pairs}")
                   ]

    def __init__(self, prevState):
        super(TryRekEmitState, self).__init__(prevState)
       
        flist = self.local['output']

        self.local['key_list'] = [settings['storage_base'] + self.in_events['metadata']['metadata'][
        'pipe_id'] + '/draw_box/' + str(uuid.uuid4()) for f in flist]
        pairs = [flist[i] + ' ' + self.local['key_list'][i] for i in xrange(len(self.local['key_list']))]
        self.commands = [s.format(**{'pairs': ' '.join(pairs)}) if s is not None else None for s in self.commands]



class GetOutputState(OnePassState):
    extra = "(check output)"
    expect = "OK:RETVAL(0)"
    command = None
    nextState = TryRekEmitState

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

        return self.nextState(self)  # don't forget this

class RunState(CommandListState):
    extra = "(run)"
    nextState = GetOutputState
    commandlist = [ (None, 'run:mkdir -p ##TMPDIR##/in_0/')
                  , ('OK:RETVAL(0)', 'collect:{in_key} ##TMPDIR##/in_0')
                  , ('OK:COLLECT', 'run:mkdir -p ##TMPDIR##/out_0/')
                  , ('OK:RETVAL(0)', 'run: python draw_box.py ' +\
                          '"{boundingbox}" ##TMPDIR##/in_0/*.png ##TMPDIR##/out_0/ 25') 
                    #check if only txt file is there 
                  , ('OK:RETVAL(0)', 'run:find ##TMPDIR##/out_0/ -type f | sort')
                    #get output in next stage
                    ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)
        self.local['dir'] = '##TMPDIR##/out_0/'
        params = {'in_key': self.in_events['metadata']['key'],
                'boundingbox':self.in_events['metadata']['boundingbox']}
        logging.debug('params: '+str(params))
        self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]


#figure out if were even going to proceed with this stage
class InitState(CommandListState):
    extra = "(init)"
    commandlist = [("OK:HELLO", "seti:nonblock:0")
        # , "run:rm -rf /tmp/*"
        , "run:mkdir -p ##TMPDIR##"
        , None
                   ]

    def __init__(self, prevState, in_events, emit_event, config):

        super(InitState, self).__init__(prevState, in_events=in_events, emit_event=emit_event, config=config, trace_func=lambda ev,msg,op:staged_trace_func("DrawBox",self.in_events['metadata']['nframes'],self.in_events['metadata']['me'],ev,msg,op))

        if self.in_events['metadata']['rek'] == False:
            self.nextState = TryNoRekEmitState
        else:
            self.nextState = RunState

        logging.debug('in_events: ' + str(in_events))

