#!/usr/bin/python

import logging
import json
from multiprocessing.pool import ThreadPool
import requests
from sprocket.platform.launcher import LauncherBase
import pdb
class Launcher(LauncherBase):
    """
    AWS Lambda launcher
    """
    @staticmethod
    def post_request(fn_name, akid, secret, payload):
        r = requests.post('https://us-central1-lixiang-project-sprocket.cloudfunctions.net/'+fn_name, data=json.loads(payload))
        logging.info(r.content)

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
