#!/usr/bin/python

from sprocket.controlling.tracker.task import Task
from sprocket.scheduler.abstract_schedulers import ConcurrencyLimitScheduler, RequestRateLimitScheduler
from sprocket.controlling.tracker.machine_state import ErrorState, TerminalState
import time 
import heapq
import subprocess

class ConcurrencyLimitPriorityScheduler(ConcurrencyLimitScheduler):

    #cur params that we hard-code for now...
    ENCODE_TIME = 10
    ENCODE_SLIVER = 0.1
    SLEEP = 1
    BUFFER = 30

    streaming_quota = 0
    start = 0
    c0_calculated = False
    c0 = 0
    end = 0
    last_lineage = 0
    expecting = 1
    delivered_lineages = [] #change to heapq or something sorted?
    awake_when = 0
    stream_difference = 0

    #calculate the theoretical streaming bound that we strive to stay under

    @classmethod
    def theoretical_bound(cls,lineage):
        return lineage + float(cls.c0) + cls.ENCODE_TIME

    @classmethod
    def task_gen(cls, pipeline, quota=0):

        encode_hopeless = False

        # get all delivered events
        all_items = []
        for key, stage in pipeline.stages.iteritems():
            while not stage.deliver_queue.empty():
                item = stage.deliver_queue.get()
                all_items.append((item, stage))

                if stage.stage_name == 'parallelize_link': #TODO: how to figure out first stage
                    cls.start = time.time()
    

        sorted_items = sorted(all_items, key=lambda item: int(item[0].values()[0]['metadata']['lineage']))
        ret = []

        sent = 0

        #print sorted_items[:2]

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

                    #cls.delivered_lineages.append(sorted_items[sent])
                    stage.deliver_queue.put(in_event)
                    sorted_items.remove(sorted_items[sent])
                    #print "--Putting back Encode Lineage: "
                    #print lineage
                    #print "Expecing: "
                    #print cls.expecting
                    ret.pop() #remove the last task from the ret task
                    #print ret
                    continue

                else:
                    cls.streaming_quota += 1 
                    cls.last_lineage = lineage

                    #figure out what the next expecting truly is
                    cls.expecting +=1
        
        for item in sorted_items[quota:]:
            item[1].deliver_queue.put(item[0])  # send back the items
        #for item in cls.delivered_lineages:
        #    item[1].deliver_queue.put(item[0])  # send back the items
        #cls.delivered_lineages = []

        #need to figure out "list index out of range"
        
        #from the previous round 
        if (quota !=0) and (cls.streaming_quota > 10):
            #calculate that line difference to see if we can sleep for at least SLEEP seconds
            bound = cls.theoretical_bound(cls.last_lineage)

            cls.end = time.time() - cls.start
            cls.stream_difference = bound - cls.ENCODE_TIME  - cls.end - cls.BUFFER
            '''
            print "Theoretical Bound"
            print bound
            print "C0"
            print cls.c0
            print "True Position"
            print cls.end
            print "Stream Difference: "
            print cls.stream_difference
            print "Streaming Quota left off on: "
            print cls.streaming_quota
            '''
            if (cls.stream_difference >= cls.SLEEP):

                print "Theoretical Bound"
                print bound
                print "C0"
                print cls.c0
                print "True Position"
                print cls.end
                print "Streaming Quota left off on: "
                print cls.streaming_quota
                print "SLEEPING for: "
                print cls.stream_difference

                #set awake when
                cls.awake_when = time.time() + cls.stream_difference 
                #time.sleep(cls.stream_difference)
                cls.streaming_quota = 0 #reset 
        
        return ret


    @classmethod
    def get_quota(cls, pipeline):
        if (time.time() <cls.awake_when):
            #cls.concurrency_limit = 1
            return 0 #we are sleeping
        else:
            cls.awake_when = 0 #reset
            return ConcurrencyLimitScheduler.get_quota(pipeline)



class RequestRateAndConcurrencyLimitPriorityScheduler(ConcurrencyLimitScheduler, RequestRateLimitScheduler):
    @classmethod
    def task_gen(cls, pipeline, n=0):
        raise NotImplementedError()

    @classmethod
    def get_quota(cls, pipeline):
        quota = min(ConcurrencyLimitScheduler.get_quota(pipeline), RequestRateLimitScheduler.get_quota(pipeline))
        return quota

    @classmethod
    def consume_quota(cls, pipeline, n=0):
        consumed = min(ConcurrencyLimitScheduler.consume_quota(pipeline, n),
                       RequestRateLimitScheduler.consume_quota(pipeline, n))
        return consumed
