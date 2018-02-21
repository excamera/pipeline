#!/usr/bin/python
from random import shuffle

from sprocket.controlling.tracker.task import Task
from sprocket.scheduler.abstract_schedulers import ConcurrencyLimitScheduler


class ConcurrencyLimitRandomScheduler(ConcurrencyLimitScheduler):
    @classmethod
    def task_gen(cls, pipeline, quota=0):
        # get all delivered events
        all_items = []
        for key, stage in pipeline.stages.iteritems():
            while not stage.deliver_queue.empty():
                item = stage.deliver_queue.get()
                all_items.append((item, stage))

        shuffle(all_items)
        ret = []
        for i in xrange(min(quota, len(all_items))):
            event = all_items[i][0]
            stage = all_items[i][1]
            t = Task(stage.lambda_function, stage.init_state, stage.event, emit_event=stage.emit,
                             in_events=event, config=stage.config, regions=stage.region)
            ret.append(t)

        for item in all_items[quota:]:
            item[1].deliver_queue.put(item[0])  # send back the items

        return ret
