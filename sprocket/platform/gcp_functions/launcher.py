#!/usr/bin/python

import os
import time
import logging
import json
from multiprocessing.pool import ThreadPool
import requests
from sprocket.platform.launcher import LauncherBase

class Launcher(LauncherBase):
    """
    AWS Lambda launcher
    """
    @staticmethod
    def post_request(fn_name, akid, secret, payload):
        d = json.loads(payload)
        d['akid'] = akid
        d['secret'] = secret
        for _ in xrange(10):
            logging.info('posting request...')
            r = requests.post('https://us-central1-lixiang-project-sprocket.cloudfunctions.net/'+fn_name, data=d)
            logging.info(r.content)
            if r.status_code != 200:
                logging.warn('request failed, retrying in 30 seconds...')
                time.sleep(30)
            else:
                break
        else:
            logging.critical('all trials failed, quiting!')
            os._exit()

    @classmethod
    def initialize(cls, launch_queue):
        """
        A blocking call to initialize the launcher.
        :param launch_queue: the event queue through which future launch events will be sent
        :return: normally does not return
        """
        requestpool = ThreadPool(processes=100)
        while True:
            launch_ev = launch_queue.get()
            for _ in xrange(launch_ev.nlaunch):
                requestpool.apply_async(Launcher.post_request, args=(launch_ev.fn_name, launch_ev.akid, launch_ev.secret, launch_ev.payload))
