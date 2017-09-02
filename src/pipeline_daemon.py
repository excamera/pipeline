import argparse
import sys
import os
import signal
import logging
import time
import pdb

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__))+'/../external/mu/src/lambdaize/')

import libmu.util
import libmu.config
import pipeline
from pipeline.config import settings
from pipeline.service import pipeline_server


def start_daemon():
    """A blocking call to start servicing pipeline"""
    libmu.config.settings = settings  # mu shares same settings
    pipeline_server.serve()

    while True:
        time.sleep(3600)  # we need to keep the the only non-daemon thread running


def shutdown(*args):
    logging.info("Shutting down daemon")
    pipeline_server.stop(0)
    exit(0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log", help="logging level", default='INFO')
    args, _ = parser.parse_known_args()
    numeric_level = getattr(logging, args.log.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.log)

    logging.basicConfig(level=numeric_level, format="%(levelname)s - %(asctime)s - %(filename)s:%(lineno)d - %(message)s")

    logging.debug("config: %s", settings)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        start_daemon()
    except KeyboardInterrupt:
        shutdown()


if __name__ == '__main__':
    main()
