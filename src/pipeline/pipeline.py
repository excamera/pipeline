import sys
import os
import logging
import math
from time import gmtime, strftime

from taskspec.generator import Generator
from taskspec.job_manager import JobManager
from util.amend_mpd import amend_mpd
from util.media_probe import get_signed_URI

import simplejson as json
import pdb

logger = logging.getLogger(__name__)

def invoke(url, commands):
    logger.info('entering pipeline.invoke')
    pipe = Generator.generate(url, commands=commands)
    JobManager.submit(pipe)
    
    os.system('aws s3 cp ' + pipe['channels'][-1]['baseURL']+'00000001_dash.mpd ' + '.')
    with open('00000001_dash.mpd', 'r') as fin:
        init_mpd = fin.read()

    duration = pipe['channels'][0]['duration']
    baseURL = pipe['channels'][-1]['baseURL']
    num_m4s = int(math.ceil(duration))
    final_mpd = amend_mpd(init_mpd, duration, baseURL, num_m4s)
    
    with open('output.xml', 'wb') as fout:
        fout.write(final_mpd)

    os.system('aws s3 cp output.xml ' + pipe['channels'][-1]['baseURL'])
    signed_mpd = get_signed_URI(pipe['channels'][-1]['baseURL']+'output.xml')
    return signed_mpd, None

if __name__=='__main__':
    pipeline = Generator.generate('s3://lixiang-lambda-test/5s.mp4', commands=[('grayscale', [])])
    JobManager.submit(pipeline)
