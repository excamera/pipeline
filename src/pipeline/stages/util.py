#!/usr/bin/python
import Queue
import logging
import libmu
from config import settings


def get_default_event():
    return {
        "mode": 1
        , "port": settings['tracker_port']
        , "addr": None  # tracker will fill this in for us
        , "nonblock": 0
        # , 'cacert': libmu.util.read_pem(settings['cacert_file']) if 'cacert_file' in settings else None
        , 'srvcrt': libmu.util.read_pem(settings['srvcrt_file']) if 'srvcrt_file' in settings else None
        , 'srvkey': libmu.util.read_pem(settings['srvkey_file']) if 'srvkey_file' in settings else None
        , 'lambda_function': settings['default_lambda_function']
    }


def default_trace_func(in_events, msg):
    """Log every command message sent by the stage.
    A command is executed after worker receives it,
    and response from worker can trigger next command,
    so time interval between two commands is a time
    upper bound for first command
    """
    logger = logging.getLogger(in_events.values()[0]['metadata']['pipe_id'])
    logger.debug(in_events.values()[0]['metadata']['lineage'] + ', ' + msg.split()[0])


def default_deliver_func(buffer_queue, deliver_queue, **kwargs):
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


def pair_deliver_func(buffer_queue, deliver_queue, stale=False, **kwargs):
    refreshed = False
    lineage_map = {}
    while not buffer_queue.empty():
        event = buffer_queue.get()
        if event.values()[0]['metadata']['lineage'] in lineage_map:
            existing_event = lineage_map.pop(event.values()[0]['metadata']['lineage'])
            paired_event = existing_event.copy()
            paired_event.update(event)
            deliver_queue.put(paired_event)
            refreshed = True
        else:
            lineage_map[event.values()[0]['metadata']['lineage']] = event

    if refreshed or not stale:
        for value in lineage_map.values():
            buffer_queue.put(value)
