#!/usr/bin/python
import logging

from libmu import tracker
from pipeline.schedule.abstact_schedulers import ConcurrencyLimitScheduler, RequestRateLimitScheduler


class ConcurrencyLimitBreadthFirstScheduler(ConcurrencyLimitScheduler):
    @classmethod
    def task_gen(cls, pipeline, quota=0):
        # get all delivered events
        ret = []
        for key, stage in pipeline.stages.iteritems():
            while not stage.deliver_queue.empty() and quota > 0:
                event = stage.deliver_queue.get()
                t = tracker.Task(stage.lambda_function, stage.init_state, event, stage.emit,
                                 stage.event, stage.config, regions=['us-east-1'])
                ret.append(t)
                quota -= 1
        return ret
