#!/usr/bin/python
import logging

from sprocket.controlling.tracker.task import Task
from sprocket.controlling.tracker.tracker import Tracker
from sprocket.scheduler.abstract_schedulers import SchedulerBase


class SimpleScheduler(SchedulerBase):

    @classmethod
    def submit_tasks(cls, pipeline, submitted):
        count_submitted = 0
        for key, stage in pipeline.stages.iteritems():
            while not stage.deliver_queue.empty():
                t = Task(stage.lambda_function, stage.init_state, stage.event, in_events=stage.deliver_queue.get(),
                         emit_event=stage.emit, config=stage.config, pipe=pipeline.pipedata, regions=stage.region)
                submitted.append(t)
                pipeline.tasks.append(t)
                Tracker.submit(t)
                count_submitted += 1
                logging.debug('submitted a task: ' + str(t))
        return count_submitted
