#!/usr/bin/python
import Queue

import logging

import time

from libmu import tracker
from libmu.machine_state import ErrorState, TerminalState


def default_deliver_func(buffer_queue, deliver_queue):
    while True:
        try:
            event = buffer_queue.get(block=False)
            deliver_queue.put(event)
            logging.debug('moving event from buffer to deliver queue')
        except Queue.Empty:
            logging.debug('finish moving, returning')
            break


class FifoScheduler(object):

    @classmethod
    def schedule(cls, pipeline):
        logging.info('start scheduling pipeline')
        last_print = 0
        tasks = []
        while True:
            buffer_empty = True
            deliver_empty = True
            for key, stage in pipeline.stages.iteritems():
                stage.deliver_func = default_deliver_func if stage.deliver_func is None else stage.deliver_func
                if not stage.buffer_queue.empty():
                    buffer_empty = False
                    stage.deliver_func(stage.buffer_queue, stage.deliver_queue)

            for key, stage in pipeline.stages.iteritems():
                while not stage.deliver_queue.empty():
                    deliver_empty = False
                    t = tracker.Task(stage.lambda_function, stage.init_state, stage.deliver_queue.get(), stage.downstreams, stage.event)
                    tasks.append(t)
                    tracker.Tracker.submit(t)
                    logging.debug('submitted a task: '+str(t))

            error_tasks = [t for t in tasks if isinstance(t.current_state, ErrorState)]
            if len(error_tasks) > 0:
                logging.error(str(len(error_tasks))+" tasks failed: ")
                for et in error_tasks:
                    logging.error(et.current_state.str_extra())
                raise Exception(str(len(error_tasks))+" tasks failed")
            tasks = [t for t in tasks if not isinstance(t.current_state, TerminalState)]

            if buffer_empty and deliver_empty and len(tasks) == 0:
                break

            if time.time() > last_print+1:
                logging.debug('current tasks:'+str(tasks))
                last_print = time.time()
            time.sleep(0.01)

        logging.info('finish scheduling pipeline')
