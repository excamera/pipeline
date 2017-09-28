#!/usr/bin/python
import Queue
import logging
from copy import deepcopy

import libmu
from pipeline.config import settings
import pdb
import heapq
import math

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


def serialized_frame_deliver_func(buffer_queues, deliver_queue, **kwargs):
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
            start += (framesperchunk) #+ (fcount - i))
            print "start"
            print start
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

def scene_deliver_func(buffer_queues, deliver_queue, **kwargs):
    """deliver scene chunks in chronoligical order when 2 scene changes are detected"""

    #puts everything back on the queue
    def helper_reset(many_events, bufferQ, pullQueue):
        for item in many_events:
            pullQueue.put(item)
        for leftover in bufferQ:
            pullQueue.put(leftover[1])
        return pullQueue

    #extracts the last used lineage
    #and returns amount of scene changes
    def extract_lineage(event, thekey):
        #if this is the first event, 
        # add a starting lineage to event
        if str(event[thekey]['seconds'][0]) == str(0):
            event[thekey]['lineage'] = 1
            lineage = 1
            scenechanges = len(event[thekey]['output'])-3 
            t1 = 0
            return event,lineage,t1,scenechanges
        else:
            t1 = float(event[thekey]['output'][-1])
            scenechanges = 0
            return event,event[thekey]['lineage'],t1,scenechanges
    

    #add a lineage for intermediate to later use
    def add_lineage(event, thekey, lineage,t1,og_scenechanges):
        FIRST_SCENE_INDEX = 3
        #calculating how many chunks we predict will be emitted
        t2 = float(event[thekey]['output'][FIRST_SCENE_INDEX])
        scenechanges = len(event[thekey]['output'])-3

        difference = math.ceil(t2 - t1)

        event[thekey]['lineage'] = int(lineage+difference+(scenechanges-1)+ og_scenechanges)
        return event


    assert len(buffer_queues) == 1  # TODO: add validator when creating pipe

    pullQueue = buffer_queues.values()[0]

    bufferQ = []

    while not pullQueue.empty():

        # first sort everything into a heap
        containsEnd = False
        sceneChange1 = False

        #take first element and extract key
        tempEvent = pullQueue.get()
        for key in tempEvent:
            thekey = key
        if str(tempEvent[thekey]['end']) == str(True):
                containsEnd = True
        heapq.heappush(bufferQ, (tempEvent[thekey]['seconds'][0],tempEvent))

        #take the rest and push onto heap
        while not pullQueue.empty():
            tempEvent = pullQueue.get()
            if str(tempEvent[thekey]['end']) == str(True):
                    containsEnd = True
            heapq.heappush(bufferQ, (tempEvent[thekey]['seconds'][0],tempEvent))
    
        #continue saving as outerevent
        metadataEvent = heapq.heappop(bufferQ)[1]
        event = {}
        event.update(metadataEvent)

        print metadataEvent

        many_events = []
        metadataEvent,lineage,t1,og_scenechanges = extract_lineage(metadataEvent,thekey)
        many_events.append(metadataEvent)

        #now append all events together
        while ((not sceneChange1) or containsEnd):

            #start taking from heap and make sure that the seconds are adjacent
            if len(bufferQ) != 0:
                event0 = heapq.heappop(bufferQ)[1]

            elif containsEnd:
                break 
            else: #if there is nothing saved in the heap, we are too early. return
                pullQueue = helper_reset(many_events, bufferQ, pullQueue)
                return 
            if event0[thekey]['seconds'][0] == many_events[-1][thekey]['seconds'][1]:
                #if there is a scene change 
                if not sceneChange1 and len(event0[thekey]['output'])>3:
                    sceneChange1 = True
                    #push the last scene change so that it is the first for
                    #the next chunk
                    event0 = add_lineage(event0,thekey, lineage,t1,og_scenechanges)
                    heapq.heappush(bufferQ, (event0[thekey]['seconds'][0],event0))
                many_events.append(event0)
            else: #we are not ready yet b/c sequential is not ready
                  #must return
                pullQueue = helper_reset(many_events, bufferQ, pullQueue)
                pullQueue.put(event0)
                return 

        #push everything back onto pullQueue that wasnt touched
        for leftover in bufferQ:
            pullQueue.put(leftover[1])

        #save allevents hidden in the metadataevent
        event[thekey]['combined_events'] = many_events
        deliver_queue.put(event)

        return

def get_output_from_message(msg):
    o_marker = '):OUTPUT('
    c_marker = '):COMMAND('
    if msg.count(o_marker) != 1 or msg.count(c_marker) != 1:
        raise Exception('incorrect message format: ' + msg)
    return msg[msg.find(o_marker) + len(o_marker):msg.find(c_marker)]


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
