#!/usr/bin/python
import Queue
import logging

#Only use on fixed length output for now...no compatibility for empty frame stuff
from copy import deepcopy

from sprocket.stages.util import preprocess_config


def smart_serialized_frame_delivery_func(buffer_queues, deliver_queue, **kwargs):
    """wait for first stage_conf['framesperchunk'] frames available frames to deliver, required for dash encode"""

    def merge_events(events, lineage):
        merged = {}
        sample = deepcopy(events[0])
        sample['frame_list']['metadata']['lineage'] = lineage

        klist = []
        for e in events:
            if e['frame_list']['key'] != None:
                klist.append(e['frame_list']['key'])

        merged['frame_list'] = {'metadata': deepcopy(sample['frame_list']['metadata']), \
                                'type': deepcopy(sample['frame_list']['type']),
                                'key_list': klist}

        # if the leftover only consisted of empty frames
        if klist == []:
            merged = {}
            return {}
        else:
            return merged

    # put together the score board of which seconds will need which frames to complete
    def makeEvents(fps, f_dic, sent_lineages):

        # insert the frames into events
        def insertEvent(events, second, key, start, up_to):
            if second not in events:
                events[second] = []
            events[second].extend([int(key), start, up_to])

        events = {}
        cur_frames = 0
        second = 1
        start = 0
        for key, amt_frames in sorted(f_dic.iteritems()):
            cur_frames += amt_frames
            if cur_frames <= fps:
                insertEvent(events, second, key, start, amt_frames)

                if cur_frames == fps:
                    cur_frames = 0  # reset where were at
                    second += 1

            else:  # we have overflowed
                start = 0
                while cur_frames > fps:

                    up_to = amt_frames - (cur_frames - fps)  # 6 - 5 = 1
                    insertEvent(events, second, key, start, up_to)
                    second += 1

                    start = up_to
                    cur_frames = amt_frames - up_to  # reset where were at in terms of count

                    # now we have overflowed a last time
                    if cur_frames <= fps:
                        up_to = start + cur_frames
                        insertEvent(events, second, key, start, up_to)

                        start = 0
                        cur_frames = cur_frames  # reset where were at in terms of count

                        if cur_frames == fps:
                            cur_frames = 0  # reset where were at
                            second += 1

        # dont add sent seconds
        for sec, v in events.items():
            if sec in sent_lineages:
                del events[sec]

        return events

    def diff(first, second):

        return [item for item in first if item not in second]

    # algorithm starts here

    assert len(buffer_queues) == 1  # TODO: add validator when creating pipe
    stale = kwargs['stale']
    stage_conf = kwargs['stage_conf']
    stage_context = kwargs['stage_context']
    pipedata = kwargs['pipedata']
    refreshed = False

    metadata = buffer_queues.values()[0].queue[0].values()[0]['metadata']
    config = preprocess_config(stage_conf, {'fps': metadata['fps']})
    fps = config.get('framesperchunk', metadata['fps'])

    # storage of what workers each second of video will need
    events = stage_context.get('events', {})
    sentLineages = stage_context.get('sentLineages', set())

    # then initialize it
    if events == {}:
        stage_context['events'] = makeEvents(fps, pipedata['frames_per_worker'], sentLineages)
        events = stage_context['events']


        # populate the available_events dictionary
    available_events = {}

    temp = []
    while not buffer_queues.values()[0].empty():
        item = buffer_queues.values()[0].get()
        lineage = int(item.values()[0]['metadata']['lineage'])
        if lineage not in available_events:
            available_events[lineage] = []
        available_events[lineage].extend([item])

    no_send = {}
    total_sent = []
    local_noSend = []

    for second in events.keys():

        # attempt to send something off
        canSend = True
        sending = []

        for i in range(0, len(events[second]), 3):  # every third will be the lineage we need
            my_second = int(events[second][i])
            if my_second not in available_events:
                canSend = False
                # put things in no send back
                for item in local_noSend:
                    buffer_queues.values()[0].put(item)
                local_noSend = []
                break

                # attempt to take from events by making list of them
            else:
                from_s = events[second][i + 1]
                to_s = events[second][i + 2]

                # sort the events properly
                sending.extend(available_events[my_second][from_s:to_s])
                no_send[my_second] = available_events[my_second][from_s:to_s]
                local_noSend.extend(available_events[my_second][from_s:to_s])
                # dont add to future events
                sentLineages.add(second)

        if canSend and not stale:
            merged = merge_events(sending, second)

            total_sent.extend(sending)

            merged['frame_list']['metadata']['duration'] = len(merged['frame_list']['key_list'])
            deliver_queue.put(merged)

            logging.info("delivered: %s", merged)
            # reset which to put back into the

            print events
            del events[second]

    stage_context['sentLineages'] = sentLineages

    # put back onto queue
    # TODO: Not entirely sure of this putting back algo
    if not stale:
        for lineage in available_events:
            toSend = diff(available_events[lineage], total_sent)  # local_noSend
            for item in toSend:
                buffer_queues.values()[0].put(item)
