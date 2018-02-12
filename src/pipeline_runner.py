#!/usr/bin/python
import os
import sys
import argparse
import logging
import grpc

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__))+'/../external/mu/src/lambdaize/')
from pipeline.config import settings
from pipeline.service import pipeline_pb2_grpc, pipeline_pb2

import pdb


def invoke_pipeline(args):
    spec = args.pipeline_spec.read()
    inputstreams = {}
    for i in args.inputs:
        # input: STREAM:TYPE:RESOURCE
        stream, inputtype, res = i.split(':', 2)
        inputs = []
        if inputtype == 'list':
            lines = open(res, 'r').read().strip().splitlines()
            inputtype = lines[0].split()[0]  # all lines should have same type
            for i in xrange(len(lines)):
                inp = pipeline_pb2.Input()
                inp.uri = lines[i].split()[1]
                inp.lineage = str(i+1)
                inputs.append(inp)
        else:
            inp = pipeline_pb2.Input()
            inp.uri = res
            inputs.append(inp)
        if stream not in inputstreams:
            instream = pipeline_pb2.InputStream()
            instream.name = stream
            instream.type = inputtype
            inputstreams[stream] = instream
        inputstreams[stream].inputs.extend(inputs)

    channel = grpc.insecure_channel('%s:%d' % (settings['daemon_addr'], settings['daemon_port']))
    stub = pipeline_pb2_grpc.PipelineStub(channel)
    response = stub.Submit(pipeline_pb2.SubmitRequest(pipeline_spec=spec, inputstreams=inputstreams.values()))
    if response.success:
        return response.mpd_url
    else:
        raise Exception(response.error_msg)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="invoke a pipeline from command line")
    parser.add_argument("pipeline_spec", metavar="PIPELINE_SPEC", help="pipeline spec file", type=argparse.FileType('r'))
    parser.add_argument("inputs", help="input url(s)", metavar="STREAM:TYPE:RESOURCE", type=str, nargs="+")
    parser.add_argument("-c", "--config", help="path to config file", type=argparse.FileType('r'),
                        default=open("pipeline_conf.json", 'r'))
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG,
                        format="%(levelname)s - %(asctime)s - %(filename)s:%(lineno)d - %(message)s")

    logging.debug("arguments: %s", args)
    logging.debug("config: %s", settings)
    logging.info("pipeline returns: %s", invoke_pipeline(args))
