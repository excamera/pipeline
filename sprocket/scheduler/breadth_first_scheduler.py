#!/usr/bin/python

from sprocket.controlling.tracker.task import Task
from sprocket.scheduler.abstract_schedulers import ConcurrencyLimitScheduler


class ConcurrencyLimitBreadthFirstScheduler(ConcurrencyLimitScheduler):
    @classmethod
    def task_gen(cls, pipeline, quota=0):
        # get all delivered events
        ret = []
        for key, stage in pipeline.stages.iteritems():
            while not stage.deliver_queue.empty() and quota > 0:
                event = stage.deliver_queue.get()
                t = Task(stage.lambda_function, stage.init_state, stage.event, in_events=event,
                                 emit_event=stage.emit, config=stage.config, pipe=pipeline.pipedata,
                                 regions=stage.region)
                ret.append(t)
                quota -= 1
        return ret
