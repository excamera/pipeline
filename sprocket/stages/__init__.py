import logging

from sprocket.config import settings
from sprocket.controlling.tracker.machine_state import CommandListState, OnePassState, TerminalState
from sprocket.stages.util import default_trace_func, get_output_from_message


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
                  , "seti:threadpool_s3:%d" % settings.get("s3_threadpool_size", 1) # s3 conn threadpool size
                  , "set:straggler_configs:%s" % settings.get("straggler_configs", '0.9 2 1') #
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


class CreateTarStateTemplate(CommandListState):
    tar_dir = None
    commandlist = [
        (None, 'run:tar -c -f {tar_dir}/archive.tar -C {tar_dir} . && find {tar_dir} -type f -not -name archive.tar -delete'),
        ('OK:RETVAL(0)', None)
    ]

    def __init__(self, prevState):
        super(CreateTarStateTemplate, self).__init__(prevState)
        self.commands = [s.format(**{'tar_dir': self.tar_dir}) if s is not None else None for s in self.commands]


class ExtractTarStateTemplate(CommandListState):
    tar_dir = None
    commandlist = [
        (None, 'run:tar -x -f {tar_dir}/archive.tar -C {tar_dir} && rm -f {tar_dir}/archive.tar'),
        ('OK:RETVAL(0)', None)
    ]

    def __init__(self, prevState):
        super(ExtractTarStateTemplate, self).__init__(prevState)
        self.commands = [s.format(**{'tar_dir': self.tar_dir}) if s is not None else None for s in self.commands]


class FinalStateTemplate(OnePassState):
    extra = "(sending quit)"
    expect = None
    command = "quit:"
    nextState = TerminalState

    def __init__(self, prevState, **kwargs):
        super(FinalStateTemplate, self).__init__(prevState)
