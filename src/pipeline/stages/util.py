#!/usr/bin/python
import Queue
import logging
import pdb

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


def default_trace_func(in_events, msg, op):
    """Log every command message sent/recv by the state machine.
    op includes send/recv/undo_recv/kick
    """
    logger = logging.getLogger(in_events.values()[0]['metadata']['pipe_id'])
    logger.debug('%s, %s, %s', in_events.values()[0]['metadata']['lineage'], op, msg.replace('\n', '\\n'))


def default_deliver_func(buffer_queues, deliver_queue, **kwargs):
    """deliver every event to deliver_queue from buffer_queue
    :param buffer_queue: output of upstream
    :param deliver_queue: input of downstream
    """
    assert len(buffer_queues) == 1  # TODO: add validator when creating pipe
    while True:
        try:
            event = buffer_queues.values()[0].get(block=False)
            deliver_queue.put(event)
            logging.debug('move an event from buffer to deliver queue')
        except Queue.Empty:
            logging.debug('finish moving, returning')
            break


def pair_deliver_func(buffer_queues, deliver_queue, **kwargs):
    """merge pairs from different queues with the same lineage and deliver them"""
    assert len(buffer_queues) == 2  # TODO: add validator when creating pipe
    stale = kwargs.get('stale', False)
    refreshed = False
    lineage_map = {}
    rebuf = []

    while not buffer_queues.values()[0].empty():
        event = buffer_queues.values()[0].get()
        lineage_map[event.values()[0]['metadata']['lineage']] = event

    while not buffer_queues.values()[1].empty():
        event = buffer_queues.values()[1].get()
        if event.values()[0]['metadata']['lineage'] in lineage_map:
            existing_event = lineage_map.pop(event.values()[0]['metadata']['lineage'])
            paired_event = existing_event.copy()
            paired_event.update(event)
            deliver_queue.put(paired_event)
            refreshed = True
        else:
            rebuf.append(event)

    if refreshed or not stale:
        for value in lineage_map.values():
            buffer_queues.values()[0].put(value)
        for event in rebuf:
            buffer_queues.values()[1].put(event)


def anypair_deliver_func(buffer_queues, deliver_queue, **kwargs):
    """merge any pairs from different channel and deliver them"""
    assert len(buffer_queues) == 2  # TODO: add validator when creating pipe
    while not buffer_queues.values()[0].empty() and not buffer_queues[1].empty():
        event0 = buffer_queues.values()[0].get()
        event1 = buffer_queues.values()[1].get()
        paired_event = {}
        paired_event.update(event0)
        paired_event.update(event1)
        deliver_queue.put(paired_event)


def get_output_from_message(msg):
    o_marker = '):OUTPUT('
    c_marker = '):COMMAND('
    if msg.count(o_marker) != 1 or msg.count(c_marker) != 1:
        raise Exception('incorrect message format: '+msg)
    return msg[msg.find(o_marker)+len(o_marker):msg.find(c_marker)]


def preprocess_config(config, existing):
    new_config = {}
    for k, v in config.iteritems():
        new_value = v.format(**existing)
        try:
            new_value = eval(new_value)
        except:
            pass
        new_config[k] = new_value
    return new_config
