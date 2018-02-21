#!/usr/bin/python
import Queue
import logging

def pair_delivery_func(buffer_queues, deliver_queue, **kwargs):
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
