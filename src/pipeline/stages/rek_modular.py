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

class EmitState(OnePassState):
    extra = "(emit)"
    expect = None
    command = None
    nextState = FinalState

    def __init__(self, prevState):
        super(EmitState, self).__init__(prevState)

    def post_transition(self):
        metadata = self.in_events['scene_list']['metadata']

        self.emit_event('metadata', {'metadata': metadata,
                                       'key_list': self.local['key_list'], #pass decode's key
                                       'fps': metadata['fps'],
                                       'lineage':metadata['lineage'],
                                       'nframes':self.in_events['scene_list']['metadata']['duration'],
                                       'boundingbox':self.local['output'],
                                       'rek': self.local['rek'], #whether rek was successful
                                       'me':metadata['lineage'],
                                       'type':self.in_events['scene_list']['type']}) 
        return self.nextState(self)  # don't forget this


class GetOutputState(OnePassState):
    extra = "(check output)"
    expect = "OK:RETVAL(0)"
    command = None
    nextState = EmitState

    def __init__(self, prevState):
        super(GetOutputState, self).__init__(prevState)

    def post_transition(self):

        matchBox = get_output_from_message(self.messages[-1])
   
        self.local['output'] = matchBox

        if str(matchBox) == "False\n":
            self.local['rek'] = False
        else:
            self.local['rek'] = True

        return self.nextState(self)  # don't forget this

class RunState(CommandListState):
    extra = "(run)"
    nextState = GetOutputState
    commandlist = [ (None, 'run:mkdir -p ##TMPDIR##/in_0/')
                  , ('OK:RETVAL(0)', 'run: python rek.py ' +\
                          '"{person}" {key_list} {bucket} 300 70 5 0.1 > ##TMPDIR##/in_0/temp.txt')
                  , ('OK:RETVAL(0)', 'run:cat ##TMPDIR##/in_0/temp.txt')
                    #get output in next stage
                    ]

    def __init__(self, prevState):
        super(RunState, self).__init__(prevState)

        self.local['bucket'] = settings['storage_base'].split('s3://')[1].split('/')[0]
        key_list = []
        for i in xrange(len(self.in_events['scene_list']['key_list'])):
            key_list.append(self.in_events['scene_list']['key_list'][i])


        self.local['key_list'] = key_list

        #stripping bucket to make keylist compatible with stage
        key_list = [k.split(self.local['bucket'])[1][1:] for k in key_list]

        params = {'key_list': ' '.join(key_list), 
                'person':self.pipe['person'],
                'bucket': self.local['bucket']}
        logging.debug('params: '+str(params))
        self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]


class InitState(CommandListState):

    nextState = RunState
    extra = "(init)"

    commandlist = [ ("OK:HELLO", "seti:nonblock:0")
                  # , "run:rm -rf /tmp/*"
                  , "run:mkdir -p ##TMPDIR##"
                  , None
                  ]

    def __init__(self, prevState, **kwargs):
        super(InitState,self).__init__(prevState, trace_func=kwargs.get('trace_func',(lambda ev,msg,op:staged_trace_func("Rek_Modular",len(self.in_events['scene_list']['key_list']), self.in_events['scene_list']['metadata']['lineage'],ev,msg,op))),**kwargs)


        lineage = int(self.in_events['scene_list']['metadata']['lineage'])
        nframes = len(self.in_events['scene_list']['key_list'])

        #populate for smart_serial delivery in encode
        self.pipe['frames_per_worker'][lineage] = nframes
        logging.debug('in_events: %s', kwargs['in_events'])


