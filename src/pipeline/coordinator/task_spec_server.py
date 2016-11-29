#!/usr/bin/python

###
# This server implements the state machine for grayscaling
# an image
#
# State Machine Description :
#  Co-ordinating TaskSpec
#   -> Configure the lambda with instance specific-settings
#   -> Retrieve each input.png from S3
#   -> Run the commands in the command-list on the retrieved files
#   -> Upload the resulting TaskSpecd png
#
# State Machine Transitions :
#  TaskSpecConfigState
#    -> TaskSpecRetrieveLoopState
#    -> TaskSpecRetrieveAndRunState
#    -> TaskSpecQuitState
#    -> FinalState
###

import os
import logging

from libmu import server, TerminalState, CommandListState, ForLoopState

logger    = logging.getLogger(__name__)
nh        = logging.NullHandler()
formatter = logging.Formatter('[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s','%m-%d %H:%M:%S')
nh.setFormatter(formatter)
logger.addHandler(nh)
logger.setLevel(logging.DEBUG)

class ServerInfo(object):
    port_number = 13579

    video_name      = "sintel-1k"
    num_frames      = 6
    num_offset      = 0
    num_parts       = 1
    lambda_function = "ffmpeg"
    regions         = ["us-east-1"]
    bucket          = "excamera-us-east-1"
    in_format       = "png16"
    out_file        = None
    profiling       = None

    cacert = None
    srvcrt = None
    srvkey = None

class FinalState(TerminalState):
    extra = "(finished)"

class TaskSpecQuitState(CommandListState):
    extra       = "(uploading)"
    nextState   = FinalState
    commandlist = [ (None, "quit:")
                  ]

class TaskSpecRetrieveAndRunState(CommandListState):
    extra       = "(retrieving png images, TaskSpec and upload)"
    commandlist = [ (None, "set:inkey:{0}/{2}.png")
                  , "set:targfile:##TMPDIR##/{2}.png"
                  , "set:cmdinfile:##TMPDIR##/{2}.png"
                  , "set:cmdoutfile:##TMPDIR##/{2}-gs.png"
                  , "set:fromfile:##TMPDIR##/{2}-gs.png"
                  , "set:outkey:{1}/{2}.png"
                  , "retrieve:"
                  , "run:./png2y4m -i -d -o ##TMPDIR##/{2}.y4m ##TMPDIR##/{2}.png"
                  , "run:./ffmpeg -i ##TMPDIR##/{2}.y4m -vf hue=s=0 -c:a copy -safe 0 ##TMPDIR##/{2}-gs.y4m"
                  , "run:./y4m2png -o ##TMPDIR##/{2}-gs.png ##TMPDIR##/{2}-gs.y4m"
                  , ("OK:RETVAL(0)", "upload:")
                  , None
                  ]

    def __init__(self, prevState, aNum=0):
        super(TaskSpecRetrieveAndRunState, self).__init__(prevState, aNum)
        inName        = "%s-%s" % (ServerInfo.video_name, ServerInfo.in_format)
        outName       = "%s-%s-%s" % (ServerInfo.video_name, ServerInfo.in_format, "TaskSpec")
        number        = 1 + ServerInfo.num_frames * (self.actorNum + ServerInfo.num_offset) + self.info['retrieve_iter']
        self.commands = [ s.format(inName, outName, "%08d" % number) if s is not None else None for s in self.commands ]

class TaskSpecRetrieveLoopState(ForLoopState):
    extra     = "(retrieve loop)"
    loopState = TaskSpecRetrieveAndRunState
    exitState = TaskSpecQuitState
    iterKey   = "retrieve_iter"

    def __init__(self, prevState, aNum=0):
        super(TaskSpecRetrieveLoopState, self).__init__(prevState, aNum)
        # number of frames to retrieve is stored in ServerInfo object
        self.iterFin = ServerInfo.num_frames

# need to set this here to avoid use-before-def
TaskSpecRetrieveAndRunState.nextState = TaskSpecRetrieveLoopState

class TaskSpecConfigState(CommandListState):
    extra       = "(configuring lambda worker)"
    nextState   = TaskSpecRetrieveLoopState
    commandlist = [ ("OK:HELLO", "seti:nonblock:0")
                  , "run:rm -rf /tmp/*"
                  , "run:mkdir -p ##TMPDIR##"
                  , None
                  ]

    def __init__(self, prevState, actorNum):
        super(TaskSpecConfigState, self).__init__(prevState, actorNum)

def run():
    # start from TaskSpecConfigState - configures lambda worker
    server.server_main_loop([], TaskSpecConfigState, ServerInfo)

def main():
    # set the server info
    server.options(ServerInfo)

    # launch the lambdas
    event = { "mode"    : 1
            , "port"    : ServerInfo.port_number
            , "addr"    : None  # server_launch will fill this in for us
            , "nonblock": 0
            , "cacert"  : ServerInfo.cacert
            , "srvcrt"  : ServerInfo.srvcrt
            , "srvkey"  : ServerInfo.srvkey
            , "bucket"  : ServerInfo.bucket
            }
    server.server_launch(ServerInfo, event, os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])

    # run the server
    run()

def convert_task_spec_to_server_info(task_spec)
    # parse the task spec
    args          = task_spec["args"]
    command       = task_spec["command"]
    input_bucket  = task_spec["input_bucket"]
    input_prefix  = task_spec["input_prefix"]
    output_bucket = task_spec["output_bucket"]
    output_prefix = task_spec["output_prefix"]
    chunk         = task_spec["input_chunk"]

    # assign to ServerInfo
    ServerInfo.num_frames      = args["args"]['-f']
    ServerInfo.num_parts       = args["args"]['-n']
    ServerInfo.bucket          = input_bucket
    ServerInfo.cacert          = args["args"]["-c"]
    ServerInfo.srvkey          = args["args"]["-k"]
    ServerInfo.srvcrt          = args["args"]["-s"]
    ServerInfo.lambda_function = args["args"]["-l"]
    ServerInfo.video_name      = args["args"]["video_name"]
    ServerInfo.in_format       = args["args"]["in_format"]

    logger.debug(ServerInfo)

def invoke(task_spec):
    # call from pipeline
    logger.debug("Running the TaskSpec...")
    convert_task_spec_to_server_info(task_spec)
    main()

if __name__ == "__main__":
    main()
