#!/usr/bin/python
import Queue
import logging

def anypair_delivery_func(buffer_queues, deliver_queue, **kwargs):
    """merge any pairs from different channel and deliver them"""
    assert len(buffer_queues) == 2  # TODO: add validator when creating pipe
    stale = kwargs['stale']
    refreshed = False
    while not buffer_queues.values()[0].empty() and not buffer_queues.values()[1].empty():
        refreshed = True
        event0 = buffer_queues.values()[0].get()
        event1 = buffer_queues.values()[1].get()
        paired_event = {}
        paired_event.update(event0)
        paired_event.update(event1)
        deliver_queue.put(paired_event)
    if not refreshed and stale:
        for q in buffer_queues.values():
            q.queue.clear()
