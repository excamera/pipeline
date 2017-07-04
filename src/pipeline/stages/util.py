#!/usr/bin/python
import logging


def default_trace_func(in_events, msg):
    logger = logging.getLogger(in_events['pipe_id'])
    logger.debug(in_events['lineage'] + ', ' + msg.split()[0])