#!/usr/bin/python

from sprocket.controlling.tracker.task import Task
from sprocket.scheduler.abstract_schedulers import ConcurrencyLimitScheduler, RequestRateLimitScheduler
from sprocket.controlling.tracker.machine_state import ErrorState, TerminalState
import time 

class StreamingScheduler(ConcurrencyLimitScheduler):

    #cur params that we hard-code for now...
    ENCODE_TIME = 10
    ENCODE_SLIVER = 0.1
    SLEEP = 1
    BUFFER = 30
    LAMBDA_LIMIT = 50

    streaming_quota = 0
    stream_limit = 10
    start = 0
    c0_calculated = False
    start_calculated = False
    c0 = 0
    end = 0
    last_lineage = 0
    expecting = 1
    delivered_lineages = [] #change to heapq or something sorted?
    awake_when = 0
    stream_difference = 0
    ready_encoders = []

    #calculate the theoretical streaming bound that we strive to stay under

    @classmethod
    def theoretical_bound(cls,lineage):
        return lineage + float(cls.c0) + cls.ENCODE_TIME

    @classmethod
    def task_gen(cls, pipeline, quota=0):

        if not cls.start_calculated:
                cls.start = time.time()
                cls.start_calculated = True
                 
        # get all delivered events
        all_items = []
        for key, stage in pipeline.stages.iteritems():
            while not stage.deliver_queue.empty():
                item = stage.deliver_queue.get()
                all_items.append((item, stage))

        sorted_items = sorted(all_items, key=lambda item: int(item[0].values()[0]['metadata']['lineage']))
        ret = []

        sent = 0
        while sent < (min(quota, len(sorted_items))):

            in_event = sorted_items[sent][0]
            stage = sorted_items[sent][1]
            t = Task(stage.lambda_function, stage.init_state, stage.event, in_events=in_event,
                             emit_event=stage.emit, config=stage.config, pipe=pipeline.pipedata,
                             regions=stage.region)

            ret.append(t)
            sent +=1

            lineage = int(in_event.values()[0]['metadata']['lineage'])

            #capture the behavior of events completing
            if stage.stage_name == 'encode_to_dash': #TODO: how to figure out last stage

                #add lineage to delivered_lineages for future bookeeping
                #save intercept for theoretical bound calculations
                if not cls.c0_calculated and lineage == 1:
                    cls.c0 = time.time() - cls.start
                    cls.c0_calculated = True

                #send the encode stage back - don't deal with it yet
                if lineage != cls.expecting:
                    sent -=1
                    stage.deliver_queue.put(in_event)
                    sorted_items.remove(sorted_items[sent])
                    ret.pop() #remove the last task from the ret task
                    continue

                else:
                    cls.streaming_quota += 1 
                    cls.last_lineage = lineage
                    #figure out what the next expecting truly is
                    cls.expecting +=1

        
        for item in sorted_items[quota:]:
            item[1].deliver_queue.put(item[0])  # send back the items

        #need to figure out "list index out of range"
        
        #from the previous round 
        if ((quota !=0) and (cls.streaming_quota > cls.stream_limit)):

            #calculate that line difference to see if we can sleep for at least SLEEP seconds
            bound = cls.theoretical_bound(cls.last_lineage)

            cls.end = time.time() - cls.start
            cls.stream_difference = bound - cls.ENCODE_TIME  - cls.end - cls.BUFFER

            cls.streaming_quota = 0 #reset 
            if (cls.stream_difference >= cls.SLEEP):

                print "***SLEEPING for*** "
                print "***" + str(cls.stream_difference) + "***"

                #set awake when
                cls.awake_when = time.time() + cls.stream_difference 
        
        return ret


    @classmethod
    def get_quota(cls, pipeline):
        if (time.time() <cls.awake_when):
            return 0 #we are sleeping
        else:
            cls.awake_when = 0 #reset
            running = [t for t in pipeline.tasks if not (isinstance(t.current_state, TerminalState) or
                                                     isinstance(t.current_state, ErrorState))]
            return cls.LAMBDA_LIMIT - len(running)

    @classmethod
    def get_deliverQueueEmpty(cls, pipeline):
        deliver_empty = True
        for key, stage in pipeline.stages.iteritems():
            if not stage.deliver_queue.empty():
                deliver_empty = False
                return deliver_empty

        return deliver_empty

