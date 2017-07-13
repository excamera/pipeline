import Queue
import sys
import os
import math
import subprocess
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.realpath(__file__))+'/../../external/mu/src/lambdaize/')

import libmu.util
from taskspec.job_manager import JobManager
from taskspec.pipeline import Pipeline
from taskspec.scheduler import SimpleScheduler, BarrierScheduler, LadderScheduler, PriorityLadderScheduler
from taskspec.generator import Generator
from util import media_probe
from stages import decode_from_url, grayscale, encode_to_dash, blend
from stages.util import pair_deliver_func
from util.amend_mpd import amend_mpd
from util.media_probe import get_signed_URI

import simplejson as json
import pdb
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(asctime)s - %(filename)s:%(lineno)d - %(message)s")

config = defaultdict(lambda: None)

with open('mu_conf.json', 'r') as f:
    c = json.load(f)
for k, v in c.iteritems():
    config[k] = v

default_event = {"mode": 1
    , "port": config['port_number']
    , "addr": None  # server_launch will fill this in for us
    , "nonblock": 0
    # , 'cacert': libmu.util.read_pem(config['cacert_file']) if config['cacert_file'] is not None else None
    , 'srvcrt': libmu.util.read_pem(config['srvcrt_file']) if config['srvcrt_file'] is not None else None
    , 'srvkey': libmu.util.read_pem(config['srvkey_file']) if config['srvkey_file'] is not None else None
         }


def get_video_url(link):
    url_list = subprocess.check_output('youtube-dl --get-url '+link, stderr=subprocess.STDOUT, shell=True).split('\n')
    for u in url_list:
        if u.startswith('http'):
            video_url = u
            break
    return video_url


def build_gs_pipeline(url):
    output = Queue.Queue()
    pipe = Pipeline()
    pipe.add_stage(Pipeline.Stage('decode', 'lambda_test_WlgU5cKP', decode_from_url.InitState, default_event))
    pipe.add_stage(Pipeline.Stage('grayscale', 'lambda_test_WlgU5cKP', grayscale.InitState, default_event))
    pipe.add_stage(Pipeline.Stage('encode', 'lambda_test_WlgU5cKP', encode_to_dash.InitState, default_event))
    pipe.add_downstream('decode', 'grayscale', 'frames')
    pipe.add_downstream('grayscale', 'encode', 'frames')
    pipe.add_downstream('encode', output, 'chunks')

    signed_URI = media_probe.get_signed_URI(url)  # currently only single input url for all workers
    duration = media_probe.get_duration(signed_URI)
    pipe.duration = duration
    fps = media_probe.get_fps(signed_URI)
    for i in range(int(math.ceil(duration))):
        inevent = {'key': signed_URI, 'starttime': i, 'duration': 1}
        pipe.stages['decode'].buffer_queue.put({'metadata': {'pipe_id': pipe.pipe_id, 'fps': fps, 'lineage': str(i+1)}, 'video_url': inevent})

    return pipe, output


def get_gs_results(pipe, output):
    num_m4s = 0
    while not output.empty():
        chunks = output.get(block=False)
        num_m4s += 1
        if int(chunks['metadata']['lineage']) == 1:
            out_key = chunks['chunks']['key']

    return {'out_key': out_key, 'duration': pipe.duration, 'num_m4s': num_m4s}  # gs is linear transform, duration unchanged


def build_blend_pipeline(url1, url2):
    output = Queue.Queue()
    pipe = Pipeline()
    pipe.add_stage(Pipeline.Stage('decode1', 'lambda_test_WlgU5cKP', decode_from_url.InitState, default_event))
    pipe.add_stage(Pipeline.Stage('decode2', 'lambda_test_WlgU5cKP', decode_from_url.InitState, default_event))
    pipe.add_stage(Pipeline.Stage('blend', 'lambda_test_WlgU5cKP', blend.InitState, default_event, deliver_func=pair_deliver_func))
    pipe.add_stage(Pipeline.Stage('encode', 'lambda_test_WlgU5cKP', encode_to_dash.InitState, default_event))
    pipe.add_downstream('decode1', 'blend', 'frames')
    pipe.add_downstream('decode2', 'blend', 'frames')
    pipe.add_downstream('blend', 'encode', 'frames')
    pipe.add_downstream('encode', output, 'chunks')

    signed_URI1 = media_probe.get_signed_URI(url1)
    duration1 = int(math.floor(media_probe.get_duration(signed_URI1)))
    fps1 = media_probe.get_fps(signed_URI1)

    signed_URI2 = media_probe.get_signed_URI(url2)
    duration2 = int(math.floor(media_probe.get_duration(signed_URI2)))
    fps2 = media_probe.get_fps(signed_URI2)

    duration = min(duration1, duration2)
    for i in range(duration):
        inevent = {'key': signed_URI1, 'starttime': i, 'duration': 1}
        pipe.stages['decode1'].buffer_queue.put({'metadata': {'pipe_id': pipe.pipe_id, 'fps': fps1, 'lineage': str(i+1)}, 'video_url': inevent})

    for i in range(duration):
        inevent = {'key': signed_URI2, 'starttime': i, 'duration': 1}
        pipe.stages['decode2'].buffer_queue.put({'metadata': {'pipe_id': pipe.pipe_id, 'fps': fps2, 'lineage': str(i+1)}, 'video_url': inevent})

    if fps1 != fps2:
        raise Exception('different fps: %f and %f, cannot blend' % (fps1, fps2))

    pipe.duration = float(duration)

    return pipe, output


def get_blend_results(pipe, output):
    num_m4s = 0
    while not output.empty():
        chunks = output.get(block=False)
        num_m4s += 1
        if int(chunks['metadata']['lineage']) == 1:
            out_key = chunks['chunks']['key']

    return {'out_key': out_key, 'duration': pipe.duration, 'num_m4s': num_m4s}  # gs is linear transform, duration unchanged


def invoke2(url, command=None):
    logging.info('entering pipeline.invoke2()')

    # pipe, output = build_gs_pipeline(url)
    pipe, output = build_blend_pipeline(get_video_url("https://www.youtube.com/watch?v=frdj1zb9sMY"), get_video_url("https://www.youtube.com/watch?v=EpUWZ1qQ5qQ"))

    pipe_dir = 'logs/' + pipe.pipe_id
    os.system('mkdir -p ' + pipe_dir)

    handler = logging.FileHandler(pipe_dir+'/log.csv')
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(created)f, %(message)s'))
    logger = logging.getLogger(pipe.pipe_id)
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    logger.info('starting pipeline')
    SimpleScheduler.schedule(pipe)
    logger.info('pipeline finished')

    # result = get_gs_results(pipe, output)
    result = get_blend_results(pipe, output)

    os.system('aws s3 cp ' + result['out_key'] +'00000001_dash.mpd '+pipe_dir+'/')
    logging.info('mpd downloaded')
    with open(pipe_dir+'/00000001_dash.mpd', 'r') as fin:
        init_mpd = fin.read()

    final_mpd = amend_mpd(init_mpd, result['duration'], result['out_key'], result['num_m4s'])

    logging.info('mpd amended')
    with open(pipe_dir+'/output.xml', 'wb') as fout:
        fout.write(final_mpd)

    os.system('aws s3 cp ' + pipe_dir+'/output.xml ' + result['out_key'])
    logging.info('mpd uploaded')
    signed_mpd = get_signed_URI(result['out_key']+'output.xml')
    logging.info('mpd signed, returning')

    return signed_mpd, None


if __name__ == '__main__':
    video_url = get_video_url(sys.argv[1])
    invoke2(video_url)
