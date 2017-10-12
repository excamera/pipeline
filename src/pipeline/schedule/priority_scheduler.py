#!/usr/bin/python
import logging

from libmu import tracker
from pipeline.schedule.abstact_schedulers import ConcurrencyLimitScheduler, RequestRateLimitScheduler


class ConcurrencyLimitPriorityScheduler(ConcurrencyLimitScheduler):
    @classmethod
    def task_gen(cls, pipeline, quota=0):
        # get all delivered events
        all_items = []
        for key, stage in pipeline.stages.iteritems():
            while not stage.deliver_queue.empty():
                item = stage.deliver_queue.get()
                all_items.append((item, stage))

        sorted_items = sorted(all_items, key=lambda item: int(item[0].values()[0]['metadata']['lineage']))
        ret = []
        for i in xrange(min(quota, len(sorted_items))):
            in_event = sorted_items[i][0]
            stage = sorted_items[i][1]
            t = tracker.Task(stage.lambda_function, stage.init_state, stage.event, in_events=in_event,
                             emit_event=stage.emit, config=stage.config, pipe=pipeline.pipedata,
                             regions=['us-east-1'])
            ret.append(t)

        for item in sorted_items[quota:]:
            item[1].deliver_queue.put(item[0])  # send back the items

        return ret


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
