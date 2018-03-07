#!/usr/bin/python
import logging
from copy import deepcopy

from sprocket.stages.util import preprocess_config


def serialized_frame_delivery_func(buffer_queues, deliver_queue, **kwargs):
    """wait for first stage_conf['framesperchunk'] frames available frames to deliver, required for dash encode"""

    def merge_events(events, lineage):
        merged = {}
        sample = deepcopy(events[0])
        sample['frame_list']['metadata']['lineage'] = lineage
        klist = [e['frame_list']['key'] for e in events]
        merged['frame_list'] = {'metadata': deepcopy(sample['frame_list']['metadata']),
                'type': deepcopy(sample['frame_list']['type']), 'key_list': klist}
        return merged

    assert len(buffer_queues) == 1  # TODO: add validator when creating pipe
    stale = kwargs['stale']
    stage_conf = kwargs['stage_conf']
    stage_context = kwargs['stage_context']
    refreshed = False

    metadata = buffer_queues.values()[0].queue[0].values()[0]['metadata']
    expecting = stage_context.get('expecting', 1)  # expecting lineage
    next_lineage = stage_context.get('next_lineage', 1)
    config = preprocess_config(stage_conf, {'fps': metadata['fps']})
    framesperchunk = config.get('framesperchunk', metadata['fps'])

    lst = []
    while not buffer_queues.values()[0].empty():
        lst.append(buffer_queues.values()[0].get())
    ordered_events = sorted(lst, key=lambda e: int(e.values()[0]['metadata']['lineage']) * 100000000 + e.values()[0][
        'number'])
    start = 0
    while True:
        if start + framesperchunk > len(ordered_events):
            break
        for i in xrange(start, start + framesperchunk):
            # if i == start + framesperchunk - 1:
            #     pdb.set_trace()
            if int(ordered_events[i].values()[0]['metadata']['lineage']) != expecting:
                break
            if ordered_events[i].values()[0]['EOF']:
                expecting += 1
        else:  # enough frames, merge and go!
            merged = merge_events(ordered_events[start:start + framesperchunk], str(next_lineage))
            deliver_queue.put(merged)
            logging.info("delivered: %s", merged)
            start += framesperchunk
            next_lineage += 1
            refreshed = True
            stage_context['expecting'] = expecting  # only when delivered should we update stage's expecting lineage
            continue
        break

    if refreshed or not stale:
        for e in ordered_events[start:]:
            buffer_queues.values()[0].put(e)  # put them back
    else:
        # that're the only frames left, deliver

        #Liz adds: still deliver in chunks (to solve bug of one large leftover)
        while start + framesperchunk < len(ordered_events):
            merged = merge_events(ordered_events[start:start+framesperchunk], str(next_lineage))
            deliver_queue.put(merged)
            logging.info("delivered leftover: %s", merged)
            start += framesperchunk
            next_lineage += 1

        merged = merge_events(ordered_events[start:], str(next_lineage))
        deliver_queue.put(merged)
        logging.info("delivered leftover: %s", merged)

    stage_context['next_lineage'] = next_lineage
