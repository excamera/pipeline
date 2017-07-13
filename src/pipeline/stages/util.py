#!/usr/bin/python
import Queue
import logging


def default_trace_func(in_events, msg):
    """Log every command message sent by the stage.
    A command is executed after worker receives it,
    and response from worker can trigger next command,
    so time interval between two commands is a time
    upper bound for first command
    """
    logger = logging.getLogger(in_events['metadata']['pipe_id'])
    logger.debug(in_events['metadata']['lineage'] + ', ' + msg.split()[0])


def default_deliver_func(buffer_queue, deliver_queue):
    """deliver every event from buffer_queue, change event from output to input
    :param buffer_queue: output of upstream
    :param deliver_queue: input of downstream
    """
    while True:
        try:
            event = buffer_queue.get(block=False)
            deliver_queue.put(event)
            logging.debug('move an event from buffer to deliver queue')
        except Queue.Empty:
            logging.debug('finish moving, returning')
            break
