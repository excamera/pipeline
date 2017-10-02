#!/usr/bin/python
import logging

from libmu import tracker
from pipeline.schedule.abstact_schedulers import SchedulerBase


class SimpleScheduler(SchedulerBase):

    @classmethod
    def submit_tasks(cls, pipeline, submitted):
        count_submitted = 0
        for key, stage in pipeline.stages.iteritems():
            while not stage.deliver_queue.empty():
                t = tracker.Task(stage.lambda_function, stage.init_state, stage.event, in_events=stage.deliver_queue.get(),
                                 emit_event=stage.emit, config=stage.config, pipewide=pipeline.pipewide, regions=['us-east-1'])
                submitted.append(t)
                pipeline.tasks.append(t)
                tracker.Tracker.submit(t)
                count_submitted += 1
                logging.debug('submitted a task: ' + str(t))
        return count_submitted
