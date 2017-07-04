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
from taskspec.scheduler import SimpleScheduler
from taskspec.generator import Generator
from util import media_probe
from stages import decode, encode, grayscale
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
    , "port": 13579
    , "addr": None  # server_launch will fill this in for us
    , "nonblock": 0
    # , 'cacert': libmu.util.read_pem(config['cacert_file']) if config['cacert_file'] is not None else None
    , 'srvcrt': libmu.util.read_pem(config['srvcrt_file']) if config['srvcrt_file'] is not None else None
    , 'srvkey': libmu.util.read_pem(config['srvkey_file']) if config['srvkey_file'] is not None else None
         }


def invoke(url, commands):
    logging.info('entering pipeline.invoke()')
    pipe = Generator.generate(url, commands=commands)
    logging.info('generated taskspec')
    JobManager.submit(pipe)

    logging.info('pipeline finished, download sample mpd')
    os.system('aws s3 cp ' + pipe['channels'][-1]['baseURL']+'00000001_dash.mpd ' + '.')
    logging.info('mpd downloaded')
    with open('00000001_dash.mpd', 'r') as fin:
        init_mpd = fin.read()

    duration = pipe['channels'][0]['duration']
    baseURL = pipe['channels'][-1]['baseURL']
    num_m4s = int(math.ceil(duration))
    final_mpd = amend_mpd(init_mpd, duration, baseURL, num_m4s)
    
    logging.info('mpd amended')
    with open('output.xml', 'wb') as fout:
        fout.write(final_mpd)

    os.system('aws s3 cp output.xml ' + pipe['channels'][-1]['baseURL'])
    logging.info('mpd uploaded')
    signed_mpd = get_signed_URI(pipe['channels'][-1]['baseURL']+'output.xml')
    logging.info('mpd siged, returing')
    return signed_mpd, None


def invoke2(url):
    logging.info('entering pipeline.invoke2()')

    output = Queue.Queue()
    pipe = Pipeline()
    pipe.add_stage(Pipeline.Stage('decode', 'lambda_test_WlgU5cKP', decode.InitState, default_event))
    pipe.add_stage(Pipeline.Stage('grayscale', 'lambda_test_WlgU5cKP', grayscale.InitState, default_event))
    pipe.add_stage(Pipeline.Stage('encode', 'lambda_test_WlgU5cKP', encode.InitState, default_event))
    pipe.add_downstream('decode', 'grayscale', 'frames')
    pipe.add_downstream('grayscale', 'encode', 'frames')
    pipe.add_downstream('encode', output, 'chunks')

    handler = logging.FileHandler(pipe.pipe_id+'.csv')
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(created)f, %(message)s'))
    logger = logging.getLogger(pipe.pipe_id)
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    signed_URI = media_probe.get_signed_URI(url)  # currently only single input url for all workers
    duration = media_probe.get_duration(signed_URI)
    for i in range(int(math.ceil(duration))):
        inevent = {'key': signed_URI, 'starttime': i, 'duration': 1}
        pipe.stages['decode'].buffer_queue.put({'lineage': str(i), 'video_url': inevent, 'pipe_id': pipe.pipe_id})

    logger.info('starting pipeline')
    SimpleScheduler.schedule(pipe)
    logger.info('pipeline finished')

    while not output.empty():
        logging.debug(output.get(block=False))


if __name__ == '__main__':
    url_list = subprocess.check_output('youtube-dl --get-url '+sys.argv[1], stderr=subprocess.STDOUT, shell=True).split('\n')
    for u in url_list:
        if u.startswith('http'):
            video_url = u
            break
    invoke2(video_url)
