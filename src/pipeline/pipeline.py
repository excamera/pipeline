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
from taskspec.scheduler import FifoScheduler
from util import media_probe
from stages import decode
from util.amend_mpd import amend_mpd
from util.media_probe import get_signed_URI

import simplejson as json
import pdb
import logging

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s - %(asctime)s - %(filename)s:%(lineno)d - %(message)s")

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
    pipe.add_stage(Pipeline.Stage('decode', 'lambda_test_397Z91UC', decode.InitState, default_event))
    pipe.add_downstream('decode', output, 'out_0')

    signed_URI = media_probe.get_signed_URI(url)  # currently only single video for all workers
    duration = media_probe.get_duration(signed_URI)
    for i in range(int(math.ceil(duration))):
        inevent = {'segment': i, 'URL': signed_URI, 'starttime': i, 'duration': 1}
        pipe.stages['decode'].buffer_queue.put(inevent)

    FifoScheduler.schedule(pipe)
    logging.info('pipeline finished')

    # os.system('aws s3 cp ' + pipe['channels'][-1]['baseURL'] + '00000001_dash.mpd ' + '.')
    # logging.info('mpd downloaded')
    # with open('00000001_dash.mpd', 'r') as fin:
    #     init_mpd = fin.read()
    #
    # duration = pipe['channels'][0]['duration']
    # baseURL = pipe['channels'][-1]['baseURL']
    # num_m4s = int(math.ceil(duration))
    # final_mpd = amend_mpd(init_mpd, duration, baseURL, num_m4s)
    #
    # logging.info('mpd amended')
    # with open('output.xml', 'wb') as fout:
    #     fout.write(final_mpd)
    #
    # os.system('aws s3 cp output.xml ' + pipe['channels'][-1]['baseURL'])
    # logging.info('mpd uploaded')
    # signed_mpd = get_signed_URI(pipe['channels'][-1]['baseURL'] + 'output.xml')
    # logging.info('mpd siged, returing')
    # return signed_mpd, None


if __name__=='__main__':
    url_list = subprocess.check_output('youtube-dl --get-url '+sys.argv[1], stderr=subprocess.STDOUT, shell=True).split('\n')
    for u in url_list:
        if u.startswith('http'):
            video_url = u
            break
    invoke2(video_url)
