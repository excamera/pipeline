#!/usr/bin/python
import Queue
import logging

def default_delivery_func(buffer_queues, deliver_queue, **kwargs):
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
