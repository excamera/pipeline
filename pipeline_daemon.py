import argparse
import sys
import os
import signal
import logging
import time
import pdb

from sprocket.config import settings
from sprocket.service import pipeline_server


def start_daemon():
    """A blocking call to start servicing pipeline"""
    pipeline_server.serve()

    while True:
        time.sleep(3600)  # we need to keep the the only non-daemon thread running


def shutdown(*args):
    logging.info("Shutting down daemon")
    pipeline_server.stop(0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log", help="logging level", default='INFO')
    args, _ = parser.parse_known_args()
    numeric_level = getattr(logging, args.log.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.log)

    logging.basicConfig(level=numeric_level, format="%(levelname)s - %(asctime)s - %(filename)s:%(lineno)d - %(message)s")

    logging.debug("config: %s", settings)

    try:
        start_daemon()
    except KeyboardInterrupt:
        os._exit(0)


if __name__ == '__main__':
    main()
