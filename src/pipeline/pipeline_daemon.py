import sys
import os
import signal
import logging
import time
import pdb

sys.path.append(os.path.dirname(os.path.realpath(__file__))+'/../../external/mu/src/lambdaize/')

import libmu.util
import libmu.config
from config import settings
from service import pipeline_server


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
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s - %(asctime)s - %(filename)s:%(lineno)d - %(message)s")

    logging.debug("config: %s", settings)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        start_daemon()
    except KeyboardInterrupt:
        shutdown()


if __name__ == '__main__':
    main()
