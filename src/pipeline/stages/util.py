#!/usr/bin/python
import logging


def default_trace_func(in_events, msg):
    """Log every command message sent by the stage.
    A command is executed after worker receives it,
    and response from worker can trigger next command,
    so time interval between two commands is a time
    upper bound for first command
    """
    logger = logging.getLogger(in_events['pipe_id'])
    logger.debug(in_events['lineage'] + ', ' + msg.split()[0])
