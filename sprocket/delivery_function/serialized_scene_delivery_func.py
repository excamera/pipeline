#!/usr/bin/python
import Queue
import logging
from copy import deepcopy

from sprocket.stages.util import preprocess_config


def serialized_scene_delivery_func(buffer_queues, deliver_queue, **kwargs):
    """merge across different scene markers"""

    def merge_events(events, lineage):
        merged = {}
        sample = deepcopy(events[0])

        for key in sample:
            thekey = key
            break


        sample[thekey]['metadata']['lineage'] = lineage
        klist = [e[thekey]['key'] for e in events]
        sample[thekey]['metadata']['duration'] = len(klist)
        merged[thekey] = {'metadata': deepcopy(sample[thekey]['metadata']),
                                'type': deepcopy(sample[thekey]['type']),
                                'key_list': klist,
                                'nframes':len(klist)}
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

    lst = []
    while not buffer_queues.values()[0].empty():
        lst.append(buffer_queues.values()[0].get())
    ordered_events = sorted(lst, key=lambda e: int(e.values()[0]['metadata']['lineage']) * 100000000 + e.values()[0][
        'number'])

    start = 0
    current = 0
    add_expecting = False
    while current < len(ordered_events):

        if int(ordered_events[current].values()[0]['metadata']['lineage']) != expecting:
            #print ordered_events[current].values()[0]['metadata']['lineage']
            break

        else:
            if ordered_events[current].values()[0]['EOF']:
                expecting+=1

            #if this isnt a scenechange, add it to the scene
            if not bool(ordered_events[current].values()[0]['switch']):
                current +=1

                continue
            else:  # enough frames, merge and go!

                current +=1
                merged = merge_events(ordered_events[start:current], str(next_lineage))
                deliver_queue.put(merged)
                #logging.info("delivered: %s", merged)
                start = current
                next_lineage += 1
                refreshed = True

                # only when delivered should we update stage's expecting lineage
                stage_context['expecting'] = expecting

                continue
            break

    #TODO: do we need to check if a scenechange was detected?
    if refreshed or not stale:
        for e in ordered_events[start:]:
            buffer_queues.values()[0].put(e)  # put them back
    else:
        # that're the only frames left, deliver
        merged = merge_events(ordered_events[start:], str(next_lineage))
        deliver_queue.put(merged)
        #logging.info("delivered leftover: %s", merged)

    stage_context['next_lineage'] = next_lineage
