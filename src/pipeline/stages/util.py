#!/usr/bin/python
import Queue
import logging
from copy import deepcopy

import libmu
from pipeline.config import settings
import pdb
import heapq
import math
from collections import OrderedDict

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

def escape_for_csv(msg):
    # see https://stackoverflow.com/a/769675/2144939
    if ',' in msg:
        msg.replace('"', '""')
        msg = '"' + msg + '"'
    return msg.replace('\n', '\\n')

def default_trace_func(in_events, msg, op):
    """Log every command message sent/recv by the state machine.
    op includes send/recv/undo_recv/kick
    """
    logger = logging.getLogger(in_events.values()[0]['metadata']['pipe_id'])
    logger.debug('%s, %s, %s', in_events.values()[0]['metadata']['lineage'], op, escape_for_csv(msg))

def staged_trace_func(stage, num_frames,worker_called, in_events, msg, op):
    """Log every command message sent/recv by the state machine.
    op includes send/recv/undo_recv/kick
    """
    logger = logging.getLogger(in_events.values()[0]['metadata']['pipe_id'])
    logger.debug('%s, %s, %s, %s, %s, %s', stage, num_frames,worker_called, in_events.values()[0]['metadata']['lineage'], op, \
            escape_for_csv(msg))

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


#Only use on fixed length output for now...no compatibility for empty frame stuff
def smart_serialized_frame_deliver_func(buffer_queues, deliver_queue, **kwargs):
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

    # put together the score board of which seconds will need which frames to complete
    def makeEvents(fps,f_dic,sent_lineages):

        #insert the frames into events
        def insertEvent(events,second,key,start,up_to):
            if second not in events:
	        events[second] = []
	    events[second].extend([int(key),start,up_to])


	events = {}
	cur_frames = 0
	second = 1
        start = 0
	for key, amt_frames in sorted(f_dic.iteritems()): 
	    cur_frames += amt_frames
	    if cur_frames <= fps:
                insertEvent(events,second,key,start,amt_frames)

		if cur_frames == fps:
		    cur_frames = 0 #reset where were at 
		    second+=1

	    else:#we have overflowed
                start = 0
                while cur_frames > fps:

		    up_to = amt_frames - (cur_frames - fps)   # 6 - 5 = 1
                    insertEvent(events,second,key,start,up_to)
		    second+=1

                    start = up_to
		    cur_frames = amt_frames - up_to #reset where were at in terms of count

                    #now we have overflowed a last time
                    if cur_frames <=fps:
                        up_to = start+cur_frames 
                        insertEvent(events,second,key,start,up_to)
                        
                        start = 0
		        cur_frames = cur_frames #reset where were at in terms of count

            		if cur_frames == fps:
		            cur_frames = 0 #reset where were at 
		            second+=1

        #dont add sent seconds 
        for sec,v in events.items():
            if sec in sent_lineages:
                del events[sec]

        return events

    def diff(first, second):

        return [item for item in first if item not in second]

    #algorithm starts here

    assert len(buffer_queues) == 1  # TODO: add validator when creating pipe
    stale = kwargs['stale']
    stage_conf = kwargs['stage_conf']
    stage_context = kwargs['stage_context']
    pipedata = kwargs['pipedata']
    refreshed = False

    metadata = buffer_queues.values()[0].queue[0].values()[0]['metadata']
    config = preprocess_config(stage_conf, {'fps': metadata['fps']})
    fps = config.get('framesperchunk', metadata['fps'])
    
    #storage of what workers each second of video will need
    events = stage_context.get( 'events', {})
    sentLineages = stage_context.get('sentLineages', set())

    #then initialize it
    if events=={}:
        stage_context['events'] =  makeEvents(fps,pipedata['frames_per_worker'],sentLineages)
        events = stage_context['events'] 


    #populate the available_events dictionary
    available_events = {}

    temp = []
    while not buffer_queues.values()[0].empty():
        item = buffer_queues.values()[0].get()
        lineage =  int(item.values()[0]['metadata']['lineage']) 
        if lineage not in available_events:
            available_events[lineage] = []
	available_events[lineage].extend([item])

    no_send = {}
    total_sent = []
    local_noSend = []

    for second in events.keys():

	#attempt to send something off
	canSend = True
	sending = []

	for i in range(0,len(events[second]),3): #every third will be the lineage we need
                my_second = int(events[second][i])
		if my_second not in available_events:
			canSend = False
                        #put things in no send back
                        for item in local_noSend:
                            buffer_queues.values()[0].put(item)
                        local_noSend = []
                        break

		#attempt to take from events by making list of them
		else: 
			from_s = events[second][i+1]
			to_s = events[second][i+2]

                        #sort the events properly
			sending.extend(available_events[my_second][from_s:to_s])
                        no_send[my_second] = available_events[my_second][from_s:to_s]
                        local_noSend.extend(available_events[my_second][from_s:to_s])
                        #dont add to future events
                        sentLineages.add(second)


	if canSend and not stale:

                merged = merge_events(sending, second)

                total_sent.extend(sending)

                merged['frame_list']['metadata']['duration'] = len(merged['frame_list']['key_list'])
        	deliver_queue.put(merged)

		logging.info("delivered: %s", merged)
                #reset which to put back into the

                print events
                del events[second] 

    
    stage_context['sentLineages'] = sentLineages

    #put back onto queue
    #TODO: Not entirely sure of this putting back algo
    if not stale:
        for lineage in available_events:
            toSend = diff(available_events[lineage],total_sent)#local_noSend
            for item in toSend:
	            buffer_queues.values()[0].put(item)


                    

def serialized_frame_deliver_func(buffer_queues, deliver_queue, **kwargs):
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
        merged = merge_events(ordered_events[start:], str(next_lineage))
        deliver_queue.put(merged)
        logging.info("delivered leftover: %s", merged)

    stage_context['next_lineage'] = next_lineage

#TODO: Fix this algo and merge with original serialized_frame_deliver
def serialized_frame_with_empty_deliver_func(buffer_queues, deliver_queue, **kwargs):
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


def serialized_scene_deliver_func(buffer_queues, deliver_queue, **kwargs):
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
                logging.info("delivered: %s", merged)
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
        logging.info("delivered leftover: %s", merged)

    stage_context['next_lineage'] = next_lineage



def get_output_from_message(msg):
    o_marker = '):OUTPUT('
    c_marker = '):COMMAND('
    if msg.count(o_marker) != 1 or msg.count(c_marker) != 1:
        raise Exception('incorrect message format: ' + msg)
    return msg[msg.find(o_marker) + len(o_marker):msg.find(c_marker)]


def preprocess_config(config, existing):
    new_config = {}
    for k, v in config.iteritems():
        try:
            new_value = v.format(**existing)
            new_value = eval(new_value)
            new_config[k] = new_value
        except:
            new_config[k] = v
    return new_config
