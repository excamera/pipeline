#!/usr/bin/python
import Queue
import logging
from copy import deepcopy

#TODO: Fix this algo and merge with original serialized_frame_deliver
from sprocket.stages.util import preprocess_config


def serialized_frame_with_empty_delivery_func(buffer_queues, deliver_queue, **kwargs):
    """wait for first stage_conf['framesperchunk'] frames available frames to deliver, required for dash encode"""

    def merge_events(events, lineage):
        merged = {}
        sample = deepcopy(events[0])
        sample['frame_list']['metadata']['lineage'] = lineage

        klist = []
        for e in events:
            if e['frame_list']['key'] != None:
                klist.append(e['frame_list']['key'])
        merged['frame_list'] = {'metadata': deepcopy(sample['frame_list']['metadata']),\
                'type': deepcopy(sample['frame_list']['type']),
                                'key_list': klist}

        #if the leftover only consisted of empty frames
        if klist == []:
            merged = {}
            return {}
        else:
            return merged

    assert len(buffer_queues) == 1  # TODO: add validator when creating pipe
    stale = kwargs['stale']
    stage_conf = kwargs['stage_conf']
    stage_context = kwargs['stage_context']
    refreshed = False

    metadata = buffer_queues.values()[0].queue[0].values()[0]['metadata']
    expecting = stage_context.get('expecting', 1)  # expecting lineage
    #print "expecting"
    #print expecting
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

        i = start
        fcount = start
        disregard = False

        if start + framesperchunk > len(ordered_events):
            break

        while i < (start + framesperchunk):
            #pdb.set_trace()

            #there is not enough extra events to cover for the empty ones
            if fcount == len(ordered_events):
                disregard = True
                break

            if int(ordered_events[fcount].values()[0]['metadata']['lineage']) != expecting:
                disregard = True
                break

            if 'Empty' in ordered_events[fcount].values()[0]:
                expecting+=1
                #do not increment i since this is empty
                fcount +=1
                continue

            if ordered_events[fcount].values()[0]['EOF']:
                expecting += 1

            fcount+=1
            i+=1


        if not disregard:  # enough frames, merge and go!
            merged = merge_events(ordered_events[start:fcount], str(next_lineage))
            deliver_queue.put(merged)
            logging.info("delivered: %s", merged)
            start += (framesperchunk + (fcount - i))
            next_lineage += 1
            refreshed = True
            stage_context['expecting'] = expecting  # only when delivered should we update stage's expecting lineage
            continue
        break

    #print kwargs

    if refreshed or not stale:
        for e in ordered_events[start:]:
            buffer_queues.values()[0].put(e)  # put them back

    else:
        # that're the only frames left, deliver
        merged = merge_events(ordered_events[start:], str(next_lineage))

        #If leftover had only empty frames
        if merged == {}:
            return

        deliver_queue.put(merged)
        logging.info("delivered leftover: %s", merged)

    stage_context['next_lineage'] = next_lineage
