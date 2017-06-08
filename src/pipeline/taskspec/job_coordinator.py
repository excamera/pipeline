#!/usr/bin/python
import os
import logging
import threading
from time import localtime, strftime, sleep
from libmu import tracker, TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState
import libmu

lambda_function_map = {
    'decode': {'name': 'lambda_test_397Z91UC',
               'downloadCmd': [(None, 'run:./ffmpeg -y -ss {starttime} -t {duration} -i "{in_URL}" -f image2 -c:v png -r 24 '
                                    '-start_number {start_number} ##TMPDIR##/%08d-filtered.png'),
                             ('OK:RETVAL(0)', None)],
               'filterLoop': False,
               'filterCmd': [None],
               'uploadCmd': [(None, "set:fromfile:##TMPDIR##/{number}-filtered.{extension}"),
                             ("OK:SET", "set:outkey:{out_dir}/{number}.{extension}"),
                             ("OK:SET", "upload:"),
                             ('OK:UPLOAD(', None)
                             ],
               'outputFmt': 'frames'
    },

    'grayscale': {'name': 'lambda_test_397Z91UC',
                'downloadCmd': [(None, 'set:inkey:{in_dir}/{number}.{extension}'),
                                ('OK:SET', 'set:targfile:##TMPDIR##/{number}.{extension}'),
                                ('OK:SET', 'retrieve:'),
                                ('OK:RETRIEV', None)],
                'filterLoop': False,
                'filterCmd': [(None, 'run:./ffmpeg -framerate 24 -start_number {start_number} -i ##TMPDIR##/%08d.png '
                                     '-vf hue=s=0 -c:a copy -safe 0 -start_number {start_number} ##TMPDIR##/%08d-filtered.png'),
                              ('OK:RETVAL(0)', None)],
                'uploadCmd': [(None, "set:fromfile:##TMPDIR##/{number}-filtered.{extension}"),
                             ("OK:SET", "set:outkey:{out_dir}/{number}.{extension}"),
                             ("OK:SET", "upload:"),
                             ('OK:UPLOAD(', None)
                             ],
                'outputFmt': 'frames'
    },

    'encode': {'name': 'lambda_test_397Z91UC',
                'downloadCmd': [(None, 'set:inkey:{in_dir}/{number}.{extension}'),
                                ('OK:SET', 'set:targfile:##TMPDIR##/{number}.{extension}'),
                                ('OK:SET', 'retrieve:'),
                                ('OK:RETRIEV', None)],
                'filterLoop': False,
                'filterCmd': [(None, 'run:./ffmpeg -framerate 24 -start_number {start_number} -i ##TMPDIR##/%08d.png '
                               '-c:v libx264 -pix_fmt yuv420p ##TMPDIR##/{number}-filtered.mp4'),
                              ('OK:RETVAL(0)', None)],
                'uploadCmd': [(None, "set:fromfile:##TMPDIR##/{number}-filtered.{extension}"),
                             ("OK:SET", "set:outkey:{out_dir}/{number}.{extension}"),
                             ("OK:SET", "upload:"),
                             ('OK:UPLOAD(', None)
                             ],
                'outputFmt': 'range'
              },

    'encode_dash': {'name': 'lambda_test_397Z91UC',
                    'downloadCmd': [(None, 'set:inkey:{in_dir}/{number}.{extension}'),
                                    ('OK:SET', 'set:targfile:##TMPDIR##/{number}.{extension}'),
                                    ('OK:SET', 'retrieve:'),
                                    ('OK:RETRIEV', None)],
                    'filterLoop': False,
                    'filterCmd': [(None, 'run:./ffmpeg -framerate 24 -start_number {start_number} -i ##TMPDIR##/%08d.png '
                                   '-c:v libx264 -pix_fmt yuv420p ##TMPDIR##/{number}.mp4'),
                                  ('OK:RETVAL(0)', 'run:cd ##TMPDIR## && $OLDPWD/MP4Box -dash 1000 -rap -segment-name seg_{number}_ ##TMPDIR##/{number}.mp4#video:id=video ##TMPDIR##/{number}.mp4#audio:id=audio && cd -'),
                                  ('OK:RETVAL(0)', 'run:python amend_m4s.py ##TMPDIR##/seg_{number}_1.m4s {number}'),
                                  ('OK:RETVAL(0)', None)],
                    'uploadCmd': [(None, "set:fromfile:##TMPDIR##/seg_{number}_1.m4s"),
                                  ("OK:SET", "set:outkey:{out_dir}/seg_{number}_1.m4s"),
                                  ("OK:SET", "upload:"),
                                  ('OK:UPLOAD(', "set:fromfile:##TMPDIR##/{number}_dash.mpd"),
                                  ("OK:SET", "set:outkey:{out_dir}/{number}_dash.mpd"),
                                  ("OK:SET", "upload:"),
                                  ('OK:UPLOAD(', "set:fromfile:##TMPDIR##/{number}_dash_init.mp4"),
                                  ("OK:SET", "set:outkey:{out_dir}/{number}_dash_init.mp4"),
                                  ("OK:SET", "upload:"),
                                  ('OK:UPLOAD(', None),
                                 ],
                    'outputFmt': 'range'
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
    bucket = "lixiang-lambda-test"
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

    ca_cert = '/home/aolx/devel/ssl/ca_cert.pem'
    server_cert = '/home/aolx/devel/ssl/server_cert.pem'
    server_key = '/home/aolx/devel/ssl/server_key.pem'


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

        # JobCoordinator.ca_cert = libmu.util.read_pem(JobCoordinator.ca_cert)
        JobCoordinator.srvcrt = libmu.util.read_pem(JobCoordinator.server_cert)
        JobCoordinator.srvkey = libmu.util.read_pem(JobCoordinator.server_key)

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
        if JobCoordinator.function_spec['outputFmt'] == 'frames':
            out_dir = '/'.join(JobCoordinator.dns[0]['URI'].split('/')[3:-1])
            number = '%08d' % (24 * self.actorNum + self.info['upload_iter'] + 1)
            extension = JobCoordinator.dns[0]['type']
            params = {'out_dir': out_dir, 'number': number, 'extension': extension}
            self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]
        elif JobCoordinator.function_spec['outputFmt'] == 'range':
            out_dir = '/'.join(JobCoordinator.dns[0]['URI'].split('/')[3:-1])
            number = '%08d' % (self.actorNum + 1)
            extension = JobCoordinator.dns[0]['type']
            params = {'out_dir': out_dir, 'number': number, 'extension': extension}
            self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]


class UploadLoopState(ForLoopState):
    extra = "(upload loop)"
    loopState = UploadRunState
    exitState = QuitState
    iterKey = "upload_iter"
    finKey = "upload_fin"

    def __init__(self, prevState, aNum=0):
        super(UploadLoopState, self).__init__(prevState, aNum)
        if JobCoordinator.function_spec['uploadCmd'] == [None]:
            self.iterFin = 0
        elif JobCoordinator.function_spec['outputFmt'] == 'range':
            self.iterFin = 1
        elif JobCoordinator.function_spec['outputFmt'] == 'frames':
            if self.info.get(self.finKey) is None:
                self.info[self.finKey] = prevState.output_count
                self.iterFin = prevState.output_count
            else:
                self.iterFin = self.info[self.finKey]


# need to set this here to avoid use-before-def
UploadRunState.nextState = UploadLoopState


class CheckOutputState(OnePassState):
    extra = "(check output)"
    command = None
    expect = 'OK:RETVAL('
    nextState = UploadLoopState
    output_count = 0

    def post_transition(self):
        self.output_count = self.messages[-1].count('\n')
        return self.nextState(self)

    def __str__(self):
        return "%d:%s, %d outputs" % (self.actorNum, self.str_extra(), self.output_count)


class GetOutputState(OnePassState):
    extra = "(find output)"
    command = "run:find ##TMPDIR##/ -type f -name '*-filtered.png'"
    expect = None
    nextState = CheckOutputState


class FilterRunState(CommandListState):
    extra = "(run filter)"
    commandlist = []

    def __init__(self, prevState, aNum=0):
        super(FilterRunState, self).__init__(prevState, aNum)
        start_number = '%08d' % (24 * self.actorNum + 1)
        number = '%08d' % (self.actorNum + 1)
        params = {'start_number': start_number, 'number': number}
        self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]


class FilterLoopState(ForLoopState):
    extra = "(filter loop)"
    loopState = FilterRunState
    exitState = GetOutputState
    iterKey = "filter_iter"
    finKey = "filter_fin"

    def __init__(self, prevState, aNum=0):
        super(FilterLoopState, self).__init__(prevState, aNum)
        if JobCoordinator.function_spec['filterCmd'] == [None]:
            self.iterFin = 0
        elif not JobCoordinator.function_spec['filterLoop']:
            self.iterFin = 1
        else:
            pass  # need to figure out output num of last state

# need to set this here to avoid use-before-def
FilterRunState.nextState = FilterLoopState


class DownloadRunState(CommandListState):
    extra = "(retrieving input)"
    commandlist = []

    def __init__(self, prevState, aNum=0):
        super(DownloadRunState, self).__init__(prevState, aNum)
        # choose which key to run next
        if JobCoordinator.ups[0]['mode'] == 'frames':
            in_dir = '/'.join(JobCoordinator.ups[0]['URI'].split('/')[3:-1])
            number = '%08d' % (24 * self.actorNum + self.info['download_iter'] + 1)
            extension = JobCoordinator.ups[0]['type']
            params = {'in_dir': in_dir, 'number': number, 'extension': extension}
            self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]

        elif JobCoordinator.ups[0]['mode'] == 'range':
            starttime = JobCoordinator.ups[0]['tuples'][self.actorNum][0]
            duration = JobCoordinator.ups[0]['tuples'][self.actorNum][1] - starttime
            in_URI = JobCoordinator.ups[0]['URI']
            start_number = '%08d' % (24 * self.actorNum + 1)
            params = {'starttime': starttime, 'duration': duration, 'in_URL': in_URI, 'start_number': start_number}
            self.commands = [ s.format(**params) if s is not None else None for s in self.commands ]


class DownloadLoopState(ForLoopState):
    extra = "(retrieve loop)"
    loopState = DownloadRunState
    exitState = FilterLoopState
    iterKey = "download_iter"
    finKey = "download_fin"

    def __init__(self, prevState, aNum=0):
        super(DownloadLoopState, self).__init__(prevState, aNum)
        # number of frames to retrieve is stored in ServerInfo object
        if JobCoordinator.function_spec['downloadCmd'] == [None]:
            self.iterFin = 0
        elif JobCoordinator.ups[0]['mode'] == 'range':
            self.iterFin = 1
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


def submit(taskspec, upstreams, downstreams):
    jc = JobCoordinator()
    jc.init(taskspec, upstreams, downstreams)

    # launch the lambdas
    event = { "mode": 1
            , "port": JobCoordinator.port_number
            , "addr": None  # server_launch will fill this in for us
            , "nonblock": 0
            #, "cacert": libmu.util.read_pem(cacertfile)
            , "srvcrt": JobCoordinator.srvcrt
            , "srvkey": JobCoordinator.srvkey
            , "bucket": JobCoordinator.bucket
            }
    logging.info("running task:" + str(taskspec))
    tasks = []
    for i in range(JobCoordinator.num_parts):
        tasks.append(tracker.Task(JobCoordinator.lambda_function, ConfigState, i, event))

    for t in tasks:
        tracker.Tracker.submit(t)

    while len(tasks) > 0:
        sleep(1)
        error_task = [t for t in tasks if isinstance(t.current_state, ErrorState)]
        if len(error_task) > 0:
            logging.error(str(len(error_task))+"tasks failed")
            raise Exception(str(len(error_task))+"tasks failed")
        tasks = [t for t in tasks if not isinstance(t.current_state, TerminalState)]
        logging.debug(str(len(tasks))+" tasks still running")
