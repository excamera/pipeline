import sys
import os
import logging
import math
import time
import subprocess

sys.path.append(os.path.dirname(os.path.realpath(__file__))+'/../../external/mu/src/lambdaize/')

from taskspec.generator import Generator
from taskspec.job_manager import JobManager
from util.amend_mpd import amend_mpd
from util.media_probe import get_signed_URI

import simplejson as json
import pdb
import logging

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s - %(asctime)s - %(filename)s:%(lineno)d - %(message)s")

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

if __name__=='__main__':
    url_list = subprocess.check_output('youtube-dl --get-url '+sys.argv[1], stderr=subprocess.STDOUT, shell=True).split('\n')
    for u in url_list:
        if u.startswith('http'):
            video_url = u
            break
    logging.debug(invoke(video_url, [('grayscale', [])]))
