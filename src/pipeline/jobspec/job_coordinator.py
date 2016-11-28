#!/usr/bin/python
import sys
import os
import logging
import simplejson as json
from libmu import server, TerminalState, CommandListState, ForLoopState

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

lambda_function_map = {
    'grayscale':'ffmpeg_KApSgDOT',
    'crawlingtext':'crawlingtext_q9f96Jgo'
}

class JobCoordinator(object):
    port_number = 13579

    video_name = "sintel-1k"
    num_frames = 6
    num_offset = 0
    num_parts = 1
    lambda_function = "crawlingtext"
    regions = ["us-east-1"]
    bucket = "excamera-us-east-1"
    in_format = "png16"
    in_chunks = None
    out_file = None
    profiling = None
    jobspec = None
    cacert = None
    srvcrt = None
    srvkey = None

    def init(self, jobspec):
        JobCoordinator.jobspec = jobspec

        JobCoordinator.port_number = 13579

        JobCoordinator.num_frames = jobspec['nframes']
        JobCoordinator.lambda_function = lambda_function_map[jobspec['operator']]

        JobCoordinator.regions = ["us-east-1"]

        JobCoordinator.num_parts = jobspec['nchunks']

        JobCoordinator.in_URI = jobspec['upstreams'][0]['URI']
        JobCoordinator.in_bucket = jobspec['upstreams'][0]['URI'].split('/')[2]
        JobCoordinator.bucket = jobspec['upstreams'][0]['URI'].split('/')[2]
        JobCoordinator.in_chunks = jobspec['upstreams'][0]['chunks']

        JobCoordinator.out_URI = jobspec['downstreams'][0]['URI']
        JobCoordinator.out_bucket = jobspec['downstreams'][0]['URI'].split('/')[2]
        JobCoordinator.out_chunks = jobspec['downstreams'][0]['chunks']

        JobCoordinator.in_format = "png16"
        JobCoordinator.out_file = None
        JobCoordinator.profiling = None

        JobCoordinator.cacert = None
        JobCoordinator.srvcrt = None
        JobCoordinator.srvkey = None

    def start(self):
        server.options(JobCoordinator)

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


    def status(self):
        pass

class FinalState(TerminalState):
    extra = "(finished)"

class QuitState(CommandListState):
    extra = "(quit)"
    nextState = FinalState
    commandlist = [ (None, "quit:")
                  ]

class RunState(CommandListState):
    extra = "(retrieving PNG, run and upload)"
    commandlist = [ (None, "set:inkey:{0}/{2}.png")
                  , "set:targfile:##TMPDIR##/{2}.png"
                  , "set:cmdinfile:##TMPDIR##/{2}.png"
                  , "set:cmdoutfile:##TMPDIR##/{2}-text.png"
                  , "set:fromfile:##TMPDIR##/{2}-text.png"
                  , "set:outkey:{1}/{2}.png"
                  , "retrieve:"
                  , "run:"
                  , ("OK:RETVAL(0)", "upload:")
                  , None
                  ]

    def __init__(self, prevState, aNum=0):
        super(RunState, self).__init__(prevState, aNum)
        # choose which key to run next
        inName = '/'.join(JobCoordinator.in_URI.split('/')[3:-1])
        outName = '/'.join(JobCoordinator.out_URI.split('/')[3:-1])
        chunks = range(JobCoordinator.in_chunks[self.actorNum][0], JobCoordinator.in_chunks[self.actorNum][1]+1)
        number = chunks[self.info['retrieve_iter']]
        self.commands = [ s.format(inName, outName, "%08d" % number) if s is not None else None for s in self.commands ]


class LoopState(ForLoopState):
    extra = "(retrieve loop)"
    loopState = RunState
    exitState = QuitState
    iterKey = "retrieve_iter"

    def __init__(self, prevState, aNum=0):
        super(LoopState, self).__init__(prevState, aNum)
        # number of frames to retrieve is stored in ServerInfo object
        self.iterFin = JobCoordinator.num_frames

# need to set this here to avoid use-before-def
RunState.nextState = LoopState


class ConfigState(CommandListState):
    extra = "(configuring lambda worker)"
    nextState = LoopState
    commandlist = [ ("OK:HELLO", "seti:nonblock:0")
                  , "run:rm -rf /tmp/*"
                  , "run:mkdir -p ##TMPDIR##"
                  , None
                  ]

    def __init__(self, prevState, actorNum):
        super(ConfigState, self).__init__(prevState, actorNum)


def run():
    server.server_main_loop([], ConfigState, JobCoordinator)


def submit(jobspec):
    js = json.loads(jobspec)
    jc = JobCoordinator()
    jc.init(js)
    server.options2(JobCoordinator, ['-n',JobCoordinator.num_parts,'-f',JobCoordinator.num_frames,'-c','/home/aolx/devel/ssl/ca_cert.pem','-s',
                                     '/home/aolx/devel/ssl/server_cert.pem','-k','/home/aolx/devel/ssl/server_key.pem'])

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

if __name__ == '__main__':
    jobspec = '''
 {
  "engine": "aws-lambda",
  "upstreams": [
   {
    "src": "src",
    "dest": 0,
    "nchunks": 10,
    "URI": "s3://lixiang-lambda-test/input/%08d.png",
    "chunks": [
     [
      1,
      6
     ],
     [
      7,
      12
     ],
     [
      13,
      18
     ],
     [
      19,
      24
     ],
     [
      25,
      30
     ],
     [
      31,
      36
     ],
     [
      37,
      42
     ],
     [
      43,
      48
     ],
     [
      49,
      54
     ],
     [
      55,
      60
     ]
    ],
    "type": "frame",
    "channel": 0
   }
  ],
  "workers": [
   {
    "output-paths": [
     {
      "chunks": [
       [
        1,
        6
       ]
      ],
      "type": "frames",
      "URI": "s3://lixiang-lambda-test/output/%08d.png",
      "channel": 1
     }
    ],
    "input-paths": [
     {
      "chunks": [
       [
        1,
        6
       ]
      ],
      "type": "frame",
      "URI": "s3://lixiang-lambda-test/input/%08d.png",
      "channel": 0
     }
    ],
    "id": 0
   },
   {
    "output-paths": [
     {
      "chunks": [
       [
        7,
        12
       ]
      ],
      "type": "frames",
      "URI": "s3://lixiang-lambda-test/output/%08d.png",
      "channel": 1
     }
    ],
    "input-paths": [
     {
      "chunks": [
       [
        7,
        12
       ]
      ],
      "type": "frame",
      "URI": "s3://lixiang-lambda-test/input/%08d.png",
      "channel": 0
     }
    ],
    "id": 1
   },
   {
    "output-paths": [
     {
      "chunks": [
       [
        13,
        18
       ]
      ],
      "type": "frames",
      "URI": "s3://lixiang-lambda-test/output/%08d.png",
      "channel": 1
     }
    ],
    "input-paths": [
     {
      "chunks": [
       [
        13,
        18
       ]
      ],
      "type": "frame",
      "URI": "s3://lixiang-lambda-test/input/%08d.png",
      "channel": 0
     }
    ],
    "id": 2
   },
   {
    "output-paths": [
     {
      "chunks": [
       [
        19,
        24
       ]
      ],
      "type": "frames",
      "URI": "s3://lixiang-lambda-test/output/%08d.png",
      "channel": 1
     }
    ],
    "input-paths": [
     {
      "chunks": [
       [
        19,
        24
       ]
      ],
      "type": "frame",
      "URI": "s3://lixiang-lambda-test/input/%08d.png",
      "channel": 0
     }
    ],
    "id": 3
   },
   {
    "output-paths": [
     {
      "chunks": [
       [
        25,
        30
       ]
      ],
      "type": "frames",
      "URI": "s3://lixiang-lambda-test/output/%08d.png",
      "channel": 1
     }
    ],
    "input-paths": [
     {
      "chunks": [
       [
        25,
        30
       ]
      ],
      "type": "frame",
      "URI": "s3://lixiang-lambda-test/input/%08d.png",
      "channel": 0
     }
    ],
    "id": 4
   },
   {
    "output-paths": [
     {
      "chunks": [
       [
        31,
        36
       ]
      ],
      "type": "frames",
      "URI": "s3://lixiang-lambda-test/output/%08d.png",
      "channel": 1
     }
    ],
    "input-paths": [
     {
      "chunks": [
       [
        31,
        36
       ]
      ],
      "type": "frame",
      "URI": "s3://lixiang-lambda-test/input/%08d.png",
      "channel": 0
     }
    ],
    "id": 5
   },
   {
    "output-paths": [
     {
      "chunks": [
       [
        37,
        42
       ]
      ],
      "type": "frames",
      "URI": "s3://lixiang-lambda-test/output/%08d.png",
      "channel": 1
     }
    ],
    "input-paths": [
     {
      "chunks": [
       [
        37,
        42
       ]
      ],
      "type": "frame",
      "URI": "s3://lixiang-lambda-test/input/%08d.png",
      "channel": 0
     }
    ],
    "id": 6
   },
   {
    "output-paths": [
     {
      "chunks": [
       [
        43,
        48
       ]
      ],
      "type": "frames",
      "URI": "s3://lixiang-lambda-test/output/%08d.png",
      "channel": 1
     }
    ],
    "input-paths": [
     {
      "chunks": [
       [
        43,
        48
       ]
      ],
      "type": "frame",
      "URI": "s3://lixiang-lambda-test/input/%08d.png",
      "channel": 0
     }
    ],
    "id": 7
   },
   {
    "output-paths": [
     {
      "chunks": [
       [
        49,
        54
       ]
      ],
      "type": "frames",
      "URI": "s3://lixiang-lambda-test/output/%08d.png",
      "channel": 1
     }
    ],
    "input-paths": [
     {
      "chunks": [
       [
        49,
        54
       ]
      ],
      "type": "frame",
      "URI": "s3://lixiang-lambda-test/input/%08d.png",
      "channel": 0
     }
    ],
    "id": 8
   },
   {
    "output-paths": [
     {
      "chunks": [
       [
        55,
        60
       ]
      ],
      "type": "frames",
      "URI": "s3://lixiang-lambda-test/output/%08d.png",
      "channel": 1
     }
    ],
    "input-paths": [
     {
      "chunks": [
       [
        55,
        60
       ]
      ],
      "type": "frame",
      "URI": "s3://lixiang-lambda-test/input/%08d.png",
      "channel": 0
     }
    ],
    "id": 9
   }
  ],
  "nchunks": 10,
  "nframes": 6,
  "operator": "crawlingtext",
  "downstreams": [
   {
    "src": 0,
    "dest": "dest",
    "nchunks": 10,
    "URI": "s3://lixiang-lambda-test/output/%08d.png",
    "chunks": [
     [
      1,
      6
     ],
     [
      7,
      12
     ],
     [
      13,
      18
     ],
     [
      19,
      24
     ],
     [
      25,
      30
     ],
     [
      31,
      36
     ],
     [
      37,
      42
     ],
     [
      43,
      48
     ],
     [
      49,
      54
     ],
     [
      55,
      60
     ]
    ],
    "type": "frames",
    "channel": 1
   }
  ],
  "pipeid": "sjf2JY1f"
 }'''
    submit(jobspec)
