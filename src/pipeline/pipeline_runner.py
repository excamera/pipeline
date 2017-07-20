#!/usr/bin/python
import argparse
import logging
import subprocess
import grpc

from config import settings
from service import pipeline_pb2_grpc, pipeline_pb2


def get_video_url(link):
    url_list = subprocess.check_output('youtube-dl --get-url '+link, stderr=subprocess.STDOUT, shell=True).split('\n')
    for u in url_list:
        if u.startswith('http'):
            video_url = u
            break
    return video_url


def invoke_pipeline(args):
    spec = args.pipeline_spec.read()

    video_urls = []
    for u in args.input_urls:
        video_urls.append(get_video_url(u))

    options = args.options

    channel = grpc.insecure_channel('%s:%d' % (settings['daemon_addr'], settings['daemon_port']))
    stub = pipeline_pb2_grpc.PipelineStub(channel)
    response = stub.Submit(pipeline_pb2.PipelineRequest(pipeline_spec=spec, input_urls=video_urls, options=options))
    if response.success:
        return response.mpd_url
    else:
        raise Exception(response.error_msg)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("pipeline_spec", help="pipeline spec file", type=argparse.FileType('r'))
    parser.add_argument("input_urls", help="input video url(s)", nargs="+")
    parser.add_argument("-o", "--options", help="pipeline options", nargs="+")
    parser.add_argument("-c", "--config", help="path to config file", type=argparse.FileType('r'),
                        default=open("pipeline_conf.json", 'r'))
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG,
                        format="%(levelname)s - %(asctime)s - %(filename)s:%(lineno)d - %(message)s")

    logging.debug("arguments: %s", args)
    logging.debug("config: %s", settings)
    logging.info("pipeline returns: %s", invoke_pipeline(args))
