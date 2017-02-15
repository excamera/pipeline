#!/usr/bin/python
import os
import logging

from libmu import server, TerminalState, CommandListState, ForLoopState, OnePassState

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

lambda_function_map = {
    'decode': {'name': 'lambda_affinity_itFtRmyk',
               'downloadCmd': [None],
               'filterCmd': [(None, 'run:./ffmpeg -y -ss {0} -t {1} -i "{2}" -f image2 -c:v png -r 24 '
                                    '##TMPDIR##/{3}-filtered.{4}'), ('OK:RETVAL(0)', None)],
               'filterLoop': False,
               'uploadCmd': [(None, "set:fromfile:##TMPDIR##/{0}-filtered.{3}"),
                             ("OK:SET", "set:outkey:{1}/{2}.{3}"),
                             ("OK:SET", "upload:"),
                             ('OK:UPLOAD(', None)
                             ],
               'outputFmt': 'frames'
    }
}


class JobCoordinator(object):
    port_number = 13579

    video_name = "sintel-1k"
    num_frames = 6
    num_offset = 0
    num_parts = 1
    lambda_function = ""
    function_spec = {}
    regions = ["us-east-1"]
    bucket = ""
    in_format = ""
    in_chunks = None
    out_file = None
    out_format = ""
    profiling = None
    jobspec = None
    ups = None
    dns = None
    cacert = None
    srvcrt = None
    srvkey = None

    ca_cert = ''
    server_cert = ''
    server_key = ''


    def init(self, jobspec, ups, dns):
        JobCoordinator.jobspec = jobspec
        JobCoordinator.ups = ups
        JobCoordinator.dns = dns
        JobCoordinator.port_number = 13579

        JobCoordinator.function_spec = lambda_function_map[JobCoordinator.jobspec['operator']]
        JobCoordinator.lambda_function = JobCoordinator.function_spec['name']

        JobCoordinator.regions = ["us-east-1"]

        JobCoordinator.num_parts = len(ups[0]['tuples'])  # currently only one upstream

        JobCoordinator.in_format = ups[0]['type']
        JobCoordinator.out_format = dns[0]['type']

    def status(self):
        pass


class FinalState(TerminalState):
    extra = "(finished)"


class QuitState(CommandListState):
    extra = "(quit)"
    nextState = FinalState
    commandlist = [ (None, "quit:")
                  ]


class UploadRunState(CommandListState):
    extra = "(do upload)"
    commandlist = []

    def __init__(self, prevState, aNum=0):
        super(UploadRunState, self).__init__(prevState, aNum)
        # choose which key to run next
        if JobCoordinator.function_spec['outputFmt'] == 'frames':
            out_dir = '/'.join(JobCoordinator.dns[0]['URI'].split('/')[3:-1])
            out_num = self.info['retrieve_iter'] + 1
            global_out_num = 24 * self.actorNum + out_num
            self.commands = [ s.format('%08d'%out_num, out_dir, '%08d'%global_out_num, JobCoordinator.out_format)
                              if s is not None else None for s in self.commands ]
        elif JobCoordinator.function_spec['outputFmt'] == 'range':
            out_dir = '/'.join(JobCoordinator.dns[0]['URI'].split('/')[3:-1])
            out_num = 1
            global_out_num = self.actorNum + out_num
            self.commands = [ s.format('%08d'%out_num, out_dir, '%08d'%global_out_num, JobCoordinator.out_format)
                              if s is not None else None for s in self.commands ]
        # print (self.commands)


class UploadLoopState(ForLoopState):
    extra = "(upload loop)"
    loopState = UploadRunState
    exitState = QuitState
    iterKey = "retrieve_iter"

    def __init__(self, prevState, aNum=0):
        super(UploadLoopState, self).__init__(prevState, aNum)
        # number of frames to retrieve is stored in ServerInfo object
        if JobCoordinator.function_spec['outputFmt'] == 'range':
            self.iterFin = 1
        elif JobCoordinator.function_spec['outputFmt'] == 'frames':
            self.iterFin = prevState.output_count
            # tuples = JobCoordinator.ups[0]['tuples'][self.actorNum]
            # if JobCoordinator.ups[0]['mode'] == 'frames':
            #     self.iterFin = tuples[1] - tuples[0] + 1
            # elif JobCoordinator.ups[0]['mode'] == 'range':
            #     self.iterFin = int(math.ceil(tuples[1] - tuples[0]) * 24)


# need to set this here to avoid use-before-def
UploadRunState.nextState = UploadLoopState # just for testing


class CheckOutputState(OnePassState):
    extra = "(check output)"
    command = None
    expect = 'OK:RETVAL('
    nextState = UploadLoopState
    output_count = 0

    def post_transition(self):
        output_count = self.messages[-1].count('\n')
        return self.nextState(self)


class GetOutputState(OnePassState):
    extra = "(ls output)"
    command = "run:find ##TMPDIR##/ -type f -name '*-filtered.png'"
    expect = None
    nextState = CheckOutputState


class FilterRunState(CommandListState):
    extra = "(run filter)"
    commandlist = []

    def __init__(self, prevState, aNum=0):
        super(FilterRunState, self).__init__(prevState, aNum)

        start = JobCoordinator.ups[0]['tuples'][self.actorNum][0]
        duration = JobCoordinator.ups[0]['tuples'][self.actorNum][1] - start
        in_URI = JobCoordinator.ups[0]['URI']
        ftype = JobCoordinator.out_format
        self.commands = [ s.format(start, duration, in_URI, "%08d", ftype) if s is not None else None for s in self.commands ]
        # print (self.commands)


class FilterLoopState(ForLoopState):
    extra = "(filter loop)"
    loopState = FilterRunState
    exitState = GetOutputState
    iterKey = "retrieve_iter"

    def __init__(self, prevState, aNum=0):
        super(FilterLoopState, self).__init__(prevState, aNum)
        # number of frames to retrieve is stored in ServerInfo object
        if JobCoordinator.function_spec['filterLoop']:
            self.iterFin = 1
        else:
            tuples = JobCoordinator.ups[0]['tuples'][self.actorNum]
            self.iterFin = tuples[1] - tuples[0] + 1

# need to set this here to avoid use-before-def
FilterRunState.nextState = FilterLoopState


class DownloadRunState(CommandListState):
    extra = "(retrieving input)"
    commandlist = []

    def __init__(self, prevState, aNum=0):
        super(DownloadRunState, self).__init__(prevState, aNum)
        # choose which key to run next
        if JobCoordinator.ups[0]['mode'] == 'frames':
            self.commands = [ s.format(0) if s is not None else None for s in self.commands ]

        elif JobCoordinator.ups[0]['mode'] == 'range':
            self.commands = [ s.format(0) if s is not None else None for s in self.commands ]


class DownloadLoopState(ForLoopState):
    extra = "(retrieve loop)"
    loopState = DownloadRunState
    exitState = FilterLoopState
    iterKey = "retrieve_iter"

    def __init__(self, prevState, aNum=0):
        super(DownloadLoopState, self).__init__(prevState, aNum)
        # number of frames to retrieve is stored in ServerInfo object
        if JobCoordinator.function_spec['downloadCmd'] == [None]:  # use ffmpeg to retrieve
            self.iterFin = 0
        else:
            tuples = JobCoordinator.ups[0]['tuples'][self.actorNum]
            self.iterFin = tuples[1] - tuples[0] + 1

# need to set this here to avoid use-before-def
DownloadRunState.nextState = DownloadLoopState


class ConfigState(CommandListState):
    extra = "(configuring lambda worker)"
    nextState = DownloadLoopState
    commandlist = [ ("OK:HELLO", "seti:nonblock:0")
                  , "run:rm -rf /tmp/*"
                  , "run:mkdir -p ##TMPDIR##"
                  , None
                  ]

    def __init__(self, prevState, actorNum):
        super(ConfigState, self).__init__(prevState, actorNum)

        # init RunStates' commands
        DownloadRunState.commandlist = JobCoordinator.function_spec['downloadCmd']
        FilterRunState.commandlist = JobCoordinator.function_spec['filterCmd']
        UploadRunState.commandlist = JobCoordinator.function_spec['uploadCmd']


def run():
    server.server_main_loop([], ConfigState, JobCoordinator)


def submit(taskspec, upstreams, downstreams):
    jc = JobCoordinator()
    jc.init(taskspec, upstreams, downstreams)
    server.options2(JobCoordinator, ['-b', JobCoordinator.bucket
                                     , '-n', JobCoordinator.num_parts
                                     , '-f', JobCoordinator.num_frames
                                     , '-c', JobCoordinator.ca_cert
                                     , '-s', JobCoordinator.server_cert
                                     , '-k', JobCoordinator.server_key
                                     , '-D'
                                     , '-O', 'states_output'
                                     , '-P', 'prof_output'
                                     ])

    # launch the lambdas
    event = { "mode": 1
            , "port": JobCoordinator.port_number
            , "addr": None  # server_launch will fill this in for us
            , "nonblock": 0
            , "cacert": JobCoordinator.cacert
            , "srvcrt": JobCoordinator.srvcrt
            , "srvkey": JobCoordinator.srvkey
            , "bucket": JobCoordinator.bucket
            }
    server.server_launch(JobCoordinator, event, os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])

    # run the server
    run()
