#!/usr/bin/python

import os

from libmu import server, TerminalState, CommandListState, ForLoopState

class ServerInfo(object):
    port_number = 13579

    video_name = "sintel-1k"
    num_frames = 6
    num_offset = 0
    num_parts = 1
    lambda_function = "crawlingtext"
    regions = ["us-east-1"]
    bucket = "excamera-us-east-1"
    in_format = "png16"
    out_file = None
    profiling = None

    cacert = None
    srvcrt = None
    srvkey = None

class FinalState(TerminalState):
    extra = "(finished)"

class CrawlingTextQuitState(CommandListState):
    extra = "(quit)"
    nextState = FinalState
    commandlist = [ (None, "quit:")
                  ]

class CrawlingTextRunState(CommandListState):
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
        super(CrawlingTextRunState, self).__init__(prevState, aNum)
        # choose which key to run next
        inName = "%s-%s" % (ServerInfo.video_name, ServerInfo.in_format)
        outName = "%s-%s-%s" % (ServerInfo.video_name, ServerInfo.in_format, "crawlingtext")
        number = 1 + ServerInfo.num_frames * (self.actorNum + ServerInfo.num_offset) + self.info['retrieve_iter']
        self.commands = [ s.format(inName, outName, "%08d" % number) if s is not None else None for s in self.commands ]

class CrawlingTextLoopState(ForLoopState):
    extra = "(retrieve loop)"
    loopState = CrawlingTextRunState
    exitState = CrawlingTextQuitState
    iterKey = "retrieve_iter"

    def __init__(self, prevState, aNum=0):
        super(CrawlingTextLoopState, self).__init__(prevState, aNum)
        # number of frames to retrieve is stored in ServerInfo object
        self.iterFin = ServerInfo.num_frames

# need to set this here to avoid use-before-def
CrawlingTextRunState.nextState = CrawlingTextLoopState

class CrawlingTextConfigState(CommandListState):
    extra = "(configuring lambda worker)"
    nextState = CrawlingTextLoopState
    commandlist = [ ("OK:HELLO", "seti:nonblock:0")
                  , "run:rm -rf /tmp/*"
                  , "run:mkdir -p ##TMPDIR##"
                  , None
                  ]

    def __init__(self, prevState, actorNum):
        super(CrawlingTextConfigState, self).__init__(prevState, actorNum)


def run():
    server.server_main_loop([], CrawlingTextConfigState, ServerInfo)

def main():
    server.options(ServerInfo)

    # launch the lambdas
    event = { "mode": 1
            , "port": ServerInfo.port_number
            , "addr": None  # server_launch will fill this in for us
            , "nonblock": 0
            , "cacert": ServerInfo.cacert
            , "srvcrt": ServerInfo.srvcrt
            , "srvkey": ServerInfo.srvkey
            , "bucket": ServerInfo.bucket
            }
    server.server_launch(ServerInfo, event, os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])

    # run the server
    run()

if __name__ == "__main__":
    main()
