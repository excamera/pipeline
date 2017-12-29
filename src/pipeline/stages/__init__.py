import logging

from libmu import CommandListState, OnePassState, TerminalState
from pipeline.stages.util import default_trace_func, get_output_from_message


class Event(object):
    """event base class"""

    class Metadata(object):
        def __init__(self):
            pass

    class Payload(object):
        def __init__(self):
            pass

    class Frames(Payload):
        def __init__(self):
            pass

    class Frame(Payload):
        def __init__(self):
            pass

    def __init__(self, metadata, *payloads):
        pass




class InitStateTemplate(CommandListState):
    extra = "(init)"
    commandlist = [ ("OK:HELLO", "seti:nonblock:0")
                  # , "run:rm -rf /tmp/*"
                  , "seti:threadpool_s3:1"
                  , "run:mkdir -p ##TMPDIR##"
                  , None
                  ]

    def __init__(self, prevState, **kwargs):
        super(InitStateTemplate, self).__init__(prevState, trace_func=kwargs.get('trace_func', default_trace_func), **kwargs)
        logging.debug('in_events: %s', kwargs['in_events'])


class GetOutputStateTemplate(OnePassState):
    extra = "(get output)"
    expect = 'OK:'
    command = None

    def post_transition(self):
        self.local['output'] = get_output_from_message(self.messages[-1])
        return self.nextState(self)

    def __init__(self, prevState):
        super(GetOutputStateTemplate, self).__init__(prevState)


class FinalStateTemplate(OnePassState):
    extra = "(sending quit)"
    expect = None
    command = "quit:"
    nextState = TerminalState

    def __init__(self, prevState):
        super(FinalStateTemplate, self).__init__(prevState)
