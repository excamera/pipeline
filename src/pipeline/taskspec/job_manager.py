#!/usr/bin/python
import os
import sys
import math
import logging
import simplejson as json

from util import media_probe
from taskspec import job_coordinator

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class JobManager(object):

    @staticmethod
    def get_inputchunks(channel, nchunks=None, nframes=None):
        if channel['type'] in ['mp4', 'mkv', 'avi', 'mov']: # video, use ffprobe to get length
            signed_URI = media_probe.get_signed_URI(channel['URI']) # currently only single video for all workers
            duration = media_probe.get_duration(signed_URI)
            if nchunks is None:
                nchunks = int(math.ceil(duration)) # an approximation to 1s/chunk
            # currently fix chunk size to 1s of video except for the last chunk
            tuples = [(i, i+1) for i in range(nchunks)]
            tuples[nchunks-1] = (tuples[nchunks-1][0], duration)
            return 'range', signed_URI, tuples
            # return 'range', signed_URI, [(i*(duration/nchunks), (i+1)*(duration/nchunks)) for i in range(nchunks)]

        if channel['type'] in ['png', 'jpg', 'bmp']: # frames
            # get # of frames if not provided
            if nframes is None:
                URI = channel['URI']
                if '%' in URI:
                    URI = URI[:URI.rfind('/')] # assume '%' in the last part of path
                nframes = media_probe.get_nframes(URI, suffix=channel['type'])
            if nchunks is None:
                nchunks = int(math.ceil(nframes/24)) # an approximation to 24 frames/chunk
            tuples = [(i*(nframes/nchunks)+1, (i+1)*(nframes/nchunks)) for i in range(nchunks)]
            tuples[nchunks-1] = (tuples[nchunks-1][0], nframes)
            return 'frames', channel['URI'], tuples  # frame range

        else:
            logger.error('unknown type')
            return None

    @staticmethod
    def schedule(job, task):
        for ups in task['upstream']:
            s = job['channels'][ups]
            if not s['probed']:
                s['mode'], s['URI'], s['tuples'] = JobManager.get_inputchunks(s, s['nchunks'])
                s['probed'] = True
        job_coordinator.submit(task, [job['channels'][ch] for ch in task['upstream']],
                               [job['channels'][ch] for ch in task['downstream']])
        for dns in task['downstream']:  # TODO: need to make sure task succeeded
            s = job['channels'][dns]
            s['ready'] = True

    @staticmethod
    def submit(job):
        task_queue = list(job['nodes'])
        i = 0
        while i < len(task_queue):
            j = 0
            while j < len(task_queue[i]['upstream']):
                if not job['channels'][task_queue[i]['upstream'][j]]['ready']:
                    break
                j += 1
            else:
                # all upstreams ready, schedule it!
                JobManager.schedule(job, task_queue[i])
                task_queue.pop(i)
                i = 0
                continue
            i += 1
        if len(task_queue) != 0:
            logger.error('cannot find ready task, broken pipeline?')
        else:
            logger.info('job completed')
