from libmu import CommandListState, OnePassState
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
                  , "run:mkdir -p ##TMPDIR##"
                  , None
                  ]

    def __init__(self, prevState, in_events, emit_event, config):
        super(InitStateTemplate, self).__init__(prevState, in_events=in_events, emit_event=emit_event, config=config, trace_func=default_trace_func)


class GetOutputStateTemplate(OnePassState):
    extra = "(get output)"
    expect = 'OK:'
    command = None

    def post_transition(self):
        self.local['output'] = get_output_from_message(self.messages[-1])
        return self.nextState(self)

    def __init__(self, prevState):
        super(GetOutputStateTemplate, self).__init__(prevState)
