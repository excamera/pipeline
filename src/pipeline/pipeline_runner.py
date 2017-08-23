#!/usr/bin/python
import argparse
import logging
import pdb
import subprocess
import grpc

from config import settings
from service import pipeline_pb2_grpc, pipeline_pb2


def invoke_pipeline(args):
    spec = args.pipeline_spec.read()
    inputs = []
    for i in range(0, len(args.inputs), 2):
        input = pipeline_pb2.Input()
        input.type = args.inputs[i]
        input.value = args.inputs[i+1]
        inputs.append(input)
    channel = grpc.insecure_channel('%s:%d' % (settings['daemon_addr'], settings['daemon_port']))
    stub = pipeline_pb2_grpc.PipelineStub(channel)
    response = stub.Submit(pipeline_pb2.SubmitRequest(pipeline_spec=spec, inputs=inputs))
    if response.success:
        return response.mpd_url
    else:
        raise Exception(response.error_msg)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="invoke a pipeline from command line")
    parser.add_argument("pipeline_spec", metavar="PIPELINE_SPEC", help="pipeline spec file", type=argparse.FileType('r'))
    parser.add_argument("inputs", help="input url(s)", metavar="INPUT_TYPE INPUT_URL", type=str, nargs="+")
    parser.add_argument("-c", "--config", help="path to config file", type=argparse.FileType('r'),
                        default=open("pipeline_conf.json", 'r'))
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG,
                        format="%(levelname)s - %(asctime)s - %(filename)s:%(lineno)d - %(message)s")

    logging.debug("arguments: %s", args)
    logging.debug("config: %s", settings)
    logging.info("pipeline returns: %s", invoke_pipeline(args))
